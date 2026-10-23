import {
  userRepository,
  conversationRepository,
  blockRepository
} from '../repositories/index.js';
import { ChatService } from '../services/ChatService.js';
import { getSupabaseClient } from '../config/storage.js';

// Global maps to track online socket connections and typing statuses
const onlineUsersMap = new Map(); // Key: userId, Value: Set of socket.ids
const userSocketsMap = new Map(); // Key: socket.id, Value: userId
const typingMap = new Map();       // Key: conversationId, Value: userId

// Keep database presence writes ordered per user. Rapid connect/disconnect events
// must not let an older write overwrite the newest connection state.
const presenceSyncQueues = new Map();
const lastPublishedPresence = new Map();

const isUserOnline = (userId) => {
  const sockets = onlineUsersMap.get(userId);
  return Boolean(sockets && sockets.size > 0);
};

const syncPresence = (io, userId) => {
  const previous = presenceSyncQueues.get(userId) || Promise.resolve();
  const task = previous.catch(() => {}).then(async () => {
    let desiredOnline = isUserOnline(userId);

    // If socket state changes while a database write is in flight, repeat with
    // the latest state before publishing presence to other clients.
    while (true) {
      const lastSeen = desiredOnline ? undefined : new Date().toISOString();
      await userRepository.updateOnlineStatus(userId, desiredOnline, lastSeen);
      const currentOnline = isUserOnline(userId);
      if (currentOnline === desiredOnline) break;
      desiredOnline = currentOnline;
    }

    const currentlyOnline = isUserOnline(userId);
    if (lastPublishedPresence.get(userId) !== currentlyOnline) {
      lastPublishedPresence.set(userId, currentlyOnline);
      io.emit('user_presence_change', {
        userId,
        isOnline: currentlyOnline,
        lastSeen: new Date().toISOString()
      });
    }
  }).catch((err) => {
    console.error('Presence sync error:', err.message);
  });

  presenceSyncQueues.set(userId, task);
  return task.finally(() => {
    if (presenceSyncQueues.get(userId) === task) {
      presenceSyncQueues.delete(userId);
    }
  });
};

export const setupSocketHandlers = (io) => {
  io.on('connection', async (socket) => {
    const userId = socket.user.id;
    userSocketsMap.set(socket.id, userId);

    // Register disconnect handling before any asynchronous database work. Otherwise,
    // a very fast disconnect can happen before its listener exists, leaving a dead
    // socket in the in-memory presence map forever.
    socket.on('disconnect', async () => {
      userSocketsMap.delete(socket.id);

      const userSockets = onlineUsersMap.get(userId);
      if (userSockets) {
        userSockets.delete(socket.id);
        if (userSockets.size === 0) {
          onlineUsersMap.delete(userId);
        }
      }

      await syncPresence(io, userId);
    });

    // Track multi-device / multi-tab socket connections per user
    if (!onlineUsersMap.has(userId)) {
      onlineUsersMap.set(userId, new Set());
    }
    onlineUsersMap.get(userId).add(socket.id);

    // Schedule presence persistence without blocking socket event registration.
    if (onlineUsersMap.get(userId).size === 1) {
      void syncPresence(io, userId);
    }

    // Join personal user room for direct signaling & notifications
    socket.join(`user:${userId}`);

    // Send initial presence state to newly connected client
    socket.emit('initial_presence_state', {
      onlineUserIds: Array.from(onlineUsersMap.keys())
    });

    // --- CONVERSATION ROOM MANAGEMENT ---
    socket.on('join_conversation', ({ conversationId }) => {
      if (conversationId) {
        socket.join(`conversation:${conversationId}`);
      }
    });

    socket.on('leave_conversation', ({ conversationId }) => {
      if (conversationId) {
        socket.leave(`conversation:${conversationId}`);
      }
    });

    // --- REAL-TIME MESSAGING ---
    socket.on('send_message', async (data) => {
      try {
        const { conversationId, recipientId, text, type, mediaUrl, fileName, fileSize, fileType, replyTo, tempId } = data;

        // Resolve or verify conversation
        let targetConvId = conversationId;
        if (!targetConvId && recipientId) {
          const conv = await ChatService.getOrCreateConversation(userId, recipientId);
          targetConvId = conv.id;
          socket.join(`conversation:${targetConvId}`);
        }

        const conv = await conversationRepository.findById(targetConvId);
        if (!conv || !conv.participants.includes(userId)) {
          return socket.emit('error', { message: 'Access denied to conversation' });
        }

        const otherUserId = conv.participants.find(id => id !== userId);

        // ENFORCE PRIVACY: Check block status before delivering message
        const isBlockedByRecipient = await blockRepository.isBlocked(otherUserId, userId);
        const isBlockedBySender = await blockRepository.isBlocked(userId, otherUserId);

        if (isBlockedByRecipient || isBlockedBySender) {
          return socket.emit('error', { message: 'Cannot send message. Communication blocked.' });
        }

        // Save message via ChatService using verified authenticated senderId (userId)
        const { message, conversation } = await ChatService.sendMessage({
          senderId: userId,
          conversationId: targetConvId,
          text,
          type,
          mediaUrl,
          fileName,
          fileSize,
          fileType,
          replyTo
        });

        if (tempId) {
          message.tempId = tempId;
        }

        // Determine recipient online/viewing status
        const isRecipientOnline = onlineUsersMap.has(otherUserId) && onlineUsersMap.get(otherUserId).size > 0;

        // Check if recipient is ACTIVELY in the conversation room (currently viewing this chat)
        const roomSockets = io.sockets.adapter.rooms.get(`conversation:${targetConvId}`);
        let isRecipientViewing = false;
        if (roomSockets && isRecipientOnline) {
          const recipientSockets = onlineUsersMap.get(otherUserId);
          for (const sId of recipientSockets) {
            if (roomSockets.has(sId)) {
              isRecipientViewing = true;
              break;
            }
          }
        }

        // Initial status update: Mark seen ONLY if recipient is actively viewing this chat
        if (isRecipientViewing) {
          await ChatService.markMessagesAsSeen(targetConvId, otherUserId);
          message.status = 'seen';
        } else if (isRecipientOnline) {
          await ChatService.markMessagesAsDelivered(targetConvId, otherUserId);
          message.status = 'delivered';
        }

        // Emit message to conversation room AND direct user rooms so unread count updates in real time
        io.to(`conversation:${targetConvId}`).to(`user:${otherUserId}`).to(`user:${userId}`).emit('receive_message', {
          message,
          conversationId: targetConvId
        });

      } catch (err) {
        console.error('Socket send_message error:', err.message);
        socket.emit('error', { message: err.message });
      }
    });

    // --- READ / SEEN / DELIVERED STATUS UPDATES ---
    socket.on('mark_seen', async ({ conversationId }) => {
      try {
        const conv = await conversationRepository.findById(conversationId);
        if (!conv || !conv.participants.includes(userId)) return;

        const updatedMessages = await ChatService.markMessagesAsSeen(conversationId, userId);
        if (updatedMessages.length > 0) {
          io.to(`conversation:${conversationId}`).to(`user:${conv.participants.find(id => id !== userId)}`).emit('messages_status_changed', {
            conversationId,
            status: 'seen',
            messageIds: updatedMessages.map(m => m.id)
          });
        }
      } catch (err) {
        console.error('Mark seen error:', err.message);
      }
    });

    // --- TYPING INDICATORS ---
    socket.on('typing_start', ({ conversationId }) => {
      typingMap.set(conversationId, userId);
      socket.to(`conversation:${conversationId}`).emit('user_typing', {
        conversationId,
        userId,
        isTyping: true
      });
    });

    socket.on('typing_stop', ({ conversationId }) => {
      if (typingMap.get(conversationId) === userId) {
        typingMap.delete(conversationId);
      }
      socket.to(`conversation:${conversationId}`).emit('user_typing', {
        conversationId,
        userId,
        isTyping: false
      });
    });

    // --- DELETE FOR ME ---
    socket.on('delete_message_for_me', async ({ messageId, conversationId }) => {
      try {
        await ChatService.deleteMessageForUser(messageId, userId);
        socket.emit('message_deleted_for_me', { messageId, conversationId });
      } catch (err) {
        console.error('Delete message for me error:', err);
      }
    });

    // --- DELETE FOR EVERYONE ---
    socket.on('delete_message_for_everyone', async ({ messageId, conversationId }) => {
      try {
        const deletedResult = await ChatService.deleteMessageForEveryone(messageId, userId);
        if (deletedResult) {
          io.to(`conversation:${conversationId}`).emit('message_deleted_for_everyone', {
            messageId,
            conversationId
          });
        }
      } catch (err) {
        console.error('Delete message for everyone error:', err.message);
      }
    });

    // --- CLEAR CHAT FOR ME ---
    socket.on('clear_chat', async ({ conversationId }) => {
      try {
        await ChatService.clearConversationForUser(conversationId, userId);
        socket.emit('chat_cleared', { conversationId });
      } catch (err) {
        console.error('Clear chat error:', err.message);
      }
    });

    // --- WEBRTC CALL SIGNALING EVENTS WITH BLOCK POLICY ENFORCEMENT ---
    socket.on('call:initiate', async ({ recipientId, callType }) => {
      const blocked = await blockRepository.isBlocked(userId, recipientId);
      if (blocked) {
        return socket.emit('call:rejected', { reason: 'unavailable' });
      }

      io.to(`user:${recipientId}`).emit('call:incoming', {
        callerId: userId,
        callerName: socket.user.displayName || socket.user.username,
        callerAvatar: socket.user.avatarUrl,
        callType
      });
    });

    socket.on('call:accept', ({ callerId, signalData }) => {
      io.to(`user:${callerId}`).emit('call:accepted', {
        signalData,
        responderId: userId
      });
    });

    socket.on('call:reject', ({ callerId, reason }) => {
      io.to(`user:${callerId}`).emit('call:rejected', {
        reason: reason || 'declined',
        responderId: userId
      });
    });

    socket.on('call:signal', ({ targetId, signalData }) => {
      io.to(`user:${targetId}`).emit('call:signal', {
        signalData,
        senderId: userId
      });
    });

    socket.on('call:end', ({ targetId }) => {
      if (targetId) {
        io.to(`user:${targetId}`).emit('call:ended', { endedBy: userId });
      }
    });

  });
};
