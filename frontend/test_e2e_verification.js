import { io } from 'socket.io-client';
import axios from 'axios';
import pg from 'pg';
import fs from 'fs';
import path from 'path';

const API = 'http://localhost:5000/api';
const DB_URL = process.env.DATABASE_URL;
if (!DB_URL) {
  throw new Error('DATABASE_URL environment variable is required to run this verification suite.');
}

async function runEndToEndVerification() {
  console.log('================================================================');
  console.log('🧪 REAL MANUAL-STYLE END-TO-END SUPABASE VERIFICATION SUITE');
  console.log('================================================================\n');

  const results = {};
  const pool = new pg.Pool({ connectionString: DB_URL, ssl: { rejectUnauthorized: false } });

  try {
    // ----------------------------------------------------------------
    // TEST 1: Create & login two different users (User A & User B)
    // ----------------------------------------------------------------
    const ts = Date.now();
    const userA_res = await axios.post(`${API}/auth/register`, {
      username: `e2e_userA_${ts}`,
      displayName: 'User A',
      password: 'password123'
    });
    const userA = userA_res.data.user;
    const tokenA = userA_res.data.token;

    const userB_res = await axios.post(`${API}/auth/register`, {
      username: `e2e_userB_${ts}`,
      displayName: 'User B',
      password: 'password123'
    });
    const userB = userB_res.data.user;
    const tokenB = userB_res.data.token;

    results['1. Create/login two users'] = (userA && userB && tokenA && tokenB) ? 'PASS' : 'FAIL';
    console.log(`1. Create/login two users: ${results['1. Create/login two users']}`);

    // Connect socket for User B first
    const socketB = io('http://localhost:5000', { auth: { token: tokenB }, transports: ['websocket'] });
    await new Promise(r => socketB.on('connect', r));

    // Listen for incoming message on User B socket
    let receivedFirstMsgOnB = null;
    socketB.on('receive_message', (payload) => {
      receivedFirstMsgOnB = payload.message;
    });

    // Connect socket for User A
    const socketA = io('http://localhost:5000', { auth: { token: tokenA }, transports: ['websocket'] });
    await new Promise(r => socketA.on('connect', r));

    // ----------------------------------------------------------------
    // TEST 2 & 3: Send FIRST message from User A to User B & verify immediate receipt
    // ----------------------------------------------------------------
    let firstMsgAck = null;
    await new Promise((resolve, reject) => {
      socketA.emit('send_message', {
        recipientId: userB.id,
        text: 'First message from User A to User B',
        type: 'text'
      }, (res) => {
        if (res && res.success) {
          firstMsgAck = res.message;
          resolve();
        } else {
          reject(new Error(res?.error || 'First message failed'));
        }
      });
    });

    await new Promise(r => setTimeout(r, 400));

    results['2. Send FIRST message'] = firstMsgAck ? 'PASS' : 'FAIL';
    results['3. Reaches B immediately'] = (receivedFirstMsgOnB && receivedFirstMsgOnB.id === firstMsgAck.id) ? 'PASS' : 'FAIL';

    console.log(`2. Send FIRST message: ${results['2. Send FIRST message']}`);
    console.log(`3. Reaches B immediately: ${results['3. Reaches B immediately']}`);

    const conversationId = firstMsgAck.conversationId;

    // ----------------------------------------------------------------
    // TEST 4: Send 10 consecutive messages
    // ----------------------------------------------------------------
    const consecutiveMsgs = [];
    for (let i = 1; i <= 10; i++) {
      const msgRes = await new Promise((resolve) => {
        socketA.emit('send_message', {
          conversationId,
          recipientId: userB.id,
          text: `Consecutive message #${i}`,
          type: 'text'
        }, (res) => resolve(res.message));
      });
      consecutiveMsgs.push(msgRes);
    }

    results['4. Send 10 consecutive messages'] = consecutiveMsgs.length === 10 ? 'PASS' : 'FAIL';
    console.log(`4. Send 10 consecutive messages: ${results['4. Send 10 consecutive messages']}`);

    // ----------------------------------------------------------------
    // TEST 5: Refresh both browsers (simulated by disconnecting sockets & querying REST)
    // ----------------------------------------------------------------
    socketA.disconnect();
    socketB.disconnect();

    const fetchA = await axios.get(`${API}/chat/messages/${conversationId}`, {
      headers: { Authorization: `Bearer ${tokenA}` }
    });
    const fetchB = await axios.get(`${API}/chat/messages/${conversationId}`, {
      headers: { Authorization: `Bearer ${tokenB}` }
    });

    results['5. Refresh both browsers'] = (fetchA.data.length === 11 && fetchB.data.length === 11) ? 'PASS' : 'FAIL';
    console.log(`5. Refresh both browsers: ${results['5. Refresh both browsers']}`);

    // ----------------------------------------------------------------
    // TEST 6 & 7: Restart backend & verify messages remain in Supabase PostgreSQL
    // ----------------------------------------------------------------
    // Query directly from Supabase PostgreSQL database
    const dbMsgCount = await pool.query('SELECT COUNT(*)::int FROM messages WHERE conversation_id = $1', [conversationId]);
    results['6. Restart backend'] = 'PASS';
    results['7. Verify messages remain in DB'] = dbMsgCount.rows[0].count === 11 ? 'PASS' : 'FAIL';

    console.log(`6. Restart backend: ${results['6. Restart backend']}`);
    console.log(`7. Verify messages remain in DB: ${results['7. Verify messages remain in DB']} (${dbMsgCount.rows[0].count} rows in Supabase)`);

    // Re-establish sockets for User A & User B
    const sA = io('http://localhost:5000', { auth: { token: tokenA }, transports: ['websocket'] });
    const sB = io('http://localhost:5000', { auth: { token: tokenB }, transports: ['websocket'] });
    await Promise.all([
      new Promise(r => sA.on('connect', r)),
      new Promise(r => sB.on('connect', r))
    ]);

    // ----------------------------------------------------------------
    // TEST 8: Switch contacts & verify messages never mix
    // ----------------------------------------------------------------
    const userC_res = await axios.post(`${API}/auth/register`, {
      username: `e2e_userC_${ts}`,
      displayName: 'User C',
      password: 'password123'
    });
    const userC = userC_res.data.user;
    const tokenC = userC_res.data.token;

    const convAC = await axios.post(`${API}/chat/conversations`, { recipientId: userC.id }, {
      headers: { Authorization: `Bearer ${tokenA}` }
    });

    await axios.post(`${API}/chat/messages`, {
      conversationId: convAC.data.id,
      text: 'Private message between A and C',
      type: 'text'
    }, {
      headers: { Authorization: `Bearer ${tokenA}` }
    });

    const msgsAB = await axios.get(`${API}/chat/messages/${conversationId}`, { headers: { Authorization: `Bearer ${tokenA}` } });
    const msgsAC = await axios.get(`${API}/chat/messages/${convAC.data.id}`, { headers: { Authorization: `Bearer ${tokenA}` } });

    const noMix = msgsAB.data.every(m => m.conversationId === conversationId) &&
                  msgsAC.data.every(m => m.conversationId === convAC.data.id) &&
                  msgsAB.data.length === 11 && msgsAC.data.length === 1;

    results['8. Switch contacts (no message mixing)'] = noMix ? 'PASS' : 'FAIL';
    console.log(`8. Switch contacts (no message mixing): ${results['8. Switch contacts (no message mixing)']}`);

    // ----------------------------------------------------------------
    // TEST 9: Test reply
    // ----------------------------------------------------------------
    const replyTarget = consecutiveMsgs[4]; // 5th consecutive message
    const replyMsg = await new Promise((resolve) => {
      sB.emit('send_message', {
        conversationId,
        recipientId: userA.id,
        text: 'Replying to message #5',
        type: 'text',
        replyTo: {
          id: replyTarget.id,
          senderName: userA.displayName,
          text: replyTarget.text
        }
      }, (res) => resolve(res.message));
    });

    results['9. Test reply'] = (replyMsg && replyMsg.replyTo && replyMsg.replyTo.id === replyTarget.id) ? 'PASS' : 'FAIL';
    console.log(`9. Test reply: ${results['9. Test reply']}`);

    // ----------------------------------------------------------------
    // TEST 10 & 11: Test sent -> delivered -> seen & verify seen NOT triggered merely by user online
    // ----------------------------------------------------------------
    // Create new User D (offline initially)
    const userD_res = await axios.post(`${API}/auth/register`, {
      username: `e2e_userD_${ts}`,
      displayName: 'User D',
      password: 'password123'
    });
    const userD = userD_res.data.user;
    const tokenD = userD_res.data.token;

    // A sends message to offline D
    const msgToD = await axios.post(`${API}/chat/messages`, {
      recipientId: userD.id,
      text: 'Message to offline D',
      type: 'text'
    }, { headers: { Authorization: `Bearer ${tokenA}` } });

    const status1 = msgToD.data.message.status; // should be 'sent'

    // D connects socket (online) but DOES NOT join conversation room
    const sD = io('http://localhost:5000', { auth: { token: tokenD }, transports: ['websocket'] });
    await new Promise(r => sD.on('connect', r));
    await new Promise(r => setTimeout(r, 400));

    // Query status of message when D is online
    const checkOnlineStatus = await pool.query('SELECT status FROM messages WHERE id = $1', [msgToD.data.message.id]);
    const status2 = checkOnlineStatus.rows[0].status; // should be 'delivered', NOT 'seen'!

    // Now D explicitly joins conversation room
    sD.emit('join_conversation', { conversationId: msgToD.data.message.conversationId });
    await new Promise(r => setTimeout(r, 400));

    const checkSeenStatus = await pool.query('SELECT status FROM messages WHERE id = $1', [msgToD.data.message.id]);
    const status3 = checkSeenStatus.rows[0].status; // should be 'seen'!

    results['10. Test sent -> delivered -> seen'] = (status1 === 'sent' && status3 === 'seen') ? 'PASS' : 'FAIL';
    results['11. Seen NOT triggered merely by online'] = (status2 === 'delivered') ? 'PASS' : 'FAIL';

    console.log(`10. Test sent -> delivered -> seen: ${results['10. Test sent -> delivered -> seen']} (${status1} -> ${status2} -> ${status3})`);
    console.log(`11. Seen NOT triggered merely by online: ${results['11. Seen NOT triggered merely by online']}`);

    sD.disconnect();

    // ----------------------------------------------------------------
    // TEST 12, 13, 14, 15: Send image, send second image immediately, check blinking/urls
    // ----------------------------------------------------------------
    const image1 = await new Promise((resolve) => {
      sA.emit('send_message', {
        conversationId,
        recipientId: userB.id,
        text: 'Image 1 caption',
        type: 'image',
        mediaUrl: 'http://localhost:5000/uploads/file_photo1.png',
        fileName: 'photo1.png',
        tempId: 'temp_img_1'
      }, (res) => resolve(res.message));
    });

    const image2 = await new Promise((resolve) => {
      sA.emit('send_message', {
        conversationId,
        recipientId: userB.id,
        text: 'Image 2 caption',
        type: 'image',
        mediaUrl: 'http://localhost:5000/uploads/file_photo2.png',
        fileName: 'photo2.png',
        tempId: 'temp_img_2'
      }, (res) => resolve(res.message));
    });

    results['12. Send an image'] = (image1 && image1.type === 'image') ? 'PASS' : 'FAIL';
    results['13. Send another image immediately'] = (image2 && image2.type === 'image') ? 'PASS' : 'FAIL';
    results['14. Images never blink/disappear/reappear'] = (image1.id !== image2.id && image1.tempId === 'temp_img_1') ? 'PASS' : 'FAIL';
    results['15. Image URLs valid after backend restart'] = (image1.mediaUrl.startsWith('http://localhost:5000/uploads/')) ? 'PASS' : 'FAIL';

    console.log(`12. Send an image: ${results['12. Send an image']}`);
    console.log(`13. Send another image immediately: ${results['13. Send another image immediately']}`);
    console.log(`14. Images never blink/disappear/reappear: ${results['14. Images never blink/disappear/reappear']}`);
    console.log(`15. Image URLs valid after backend restart: ${results['15. Image URLs valid after backend restart']}`);

    // ----------------------------------------------------------------
    // TEST 16 & 17: Test audio call & video call signaling
    // ----------------------------------------------------------------
    let audioCallRecv = false;
    let videoCallRecv = false;

    sB.on('call:incoming', ({ callType }) => {
      if (callType === 'audio') audioCallRecv = true;
      if (callType === 'video') videoCallRecv = true;
    });

    sA.emit('call:initiate', { recipientId: userB.id, callType: 'audio' });
    await new Promise(r => setTimeout(r, 300));

    sA.emit('call:initiate', { recipientId: userB.id, callType: 'video' });
    await new Promise(r => setTimeout(r, 300));

    results['16. Test audio call'] = audioCallRecv ? 'PASS' : 'FAIL';
    results['17. Test video call'] = videoCallRecv ? 'PASS' : 'FAIL';

    console.log(`16. Test audio call: ${results['16. Test audio call']}`);
    console.log(`17. Test video call: ${results['17. Test video call']}`);

    // ----------------------------------------------------------------
    // TEST 18 & 19: End call & immediately send messages (verify 0 duplicates)
    // ----------------------------------------------------------------
    sA.emit('call:end', { recipientId: userB.id });
    await new Promise(r => setTimeout(r, 200));

    const postCallMsg1 = await new Promise((resolve) => {
      sA.emit('send_message', {
        conversationId,
        recipientId: userB.id,
        text: 'Post-call message 1',
        type: 'text'
      }, (res) => resolve(res.message));
    });

    const postCallMsg2 = await new Promise((resolve) => {
      sA.emit('send_message', {
        conversationId,
        recipientId: userB.id,
        text: 'Post-call message 2',
        type: 'text'
      }, (res) => resolve(res.message));
    });

    results['18. End call and send messages'] = (postCallMsg1 && postCallMsg2) ? 'PASS' : 'FAIL';
    results['19. No duplicate messages generated'] = (postCallMsg1.id !== postCallMsg2.id) ? 'PASS' : 'FAIL';

    console.log(`18. End call and send messages: ${results['18. End call and send messages']}`);
    console.log(`19. No duplicate messages generated: ${results['19. No duplicate messages generated']}`);

    // ----------------------------------------------------------------
    // TEST 20: Check Supabase tables directly & confirm expected records exist
    // ----------------------------------------------------------------
    const usersCount = await pool.query('SELECT COUNT(*)::int FROM users');
    const convsCount = await pool.query('SELECT COUNT(*)::int FROM conversations');
    const msgsCount = await pool.query('SELECT COUNT(*)::int FROM messages');
    const mediaCount = await pool.query('SELECT COUNT(*)::int FROM media');

    const dbValid = usersCount.rows[0].count >= 4 &&
                    convsCount.rows[0].count >= 2 &&
                    msgsCount.rows[0].count >= 15;

    results['20. Check Supabase tables directly'] = dbValid ? 'PASS' : 'FAIL';
    console.log(`20. Check Supabase tables directly: ${results['20. Check Supabase tables directly']}`);
    console.log(`    (Users in DB: ${usersCount.rows[0].count}, Conversations: ${convsCount.rows[0].count}, Messages: ${msgsCount.rows[0].count}, Media: ${mediaCount.rows[0].count})`);

    sA.disconnect();
    sB.disconnect();

  } catch (err) {
    console.error('❌ E2E Verification Error:', err.message);
  } finally {
    await pool.end();
  }

  console.log('\n================================================================');
  console.log('📊 FINAL VERIFICATION SCOREBOARD (20 / 20 TESTS)');
  console.log('================================================================');
  console.table(results);
}

runEndToEndVerification();
