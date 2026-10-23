import 'dotenv/config';
import express from 'express';
import http from 'http';
import { Server } from 'socket.io';
import cors from 'cors';
import helmet from 'helmet';
import path from 'path';
import { fileURLToPath } from 'url';

import authRoutes from './routes/authRoutes.js';
import userRoutes from './routes/userRoutes.js';
import chatRoutes from './routes/chatRoutes.js';
import privacyRoutes from './routes/privacyRoutes.js';

import { socketAuthMiddleware } from './middleware/authMiddleware.js';
import { setupSocketHandlers } from './sockets/socketHandler.js';
import { apiLimiter, authLimiter } from './middleware/rateLimiter.js';
import { initializeDatabase, query } from './config/db.js';
import { initializeStorageBucket } from './config/storage.js';
import { ChatService } from './services/ChatService.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const app = express();
const PORT = process.env.PORT || 5000;
const CORS_ORIGIN = process.env.CORS_ORIGIN || 'http://localhost:5173';

// Security Headers (Helmet) configured to allow static media & WebRTC cross-origin policies
app.use(helmet({
  crossOriginResourcePolicy: { policy: 'cross-origin' },
  contentSecurityPolicy: false
}));

// Allowed Origins for Production (Vercel & DuckDNS) and Local Development
const allowedOrigins = [
  CORS_ORIGIN,
  'https://aisolver-sigma.vercel.app',
  'https://learnai1234.duckdns.org',
  'http://localhost:5173',
  'http://127.0.0.1:5173'
];

const checkOrigin = (origin, callback) => {
  if (!origin) {
    return callback(null, true);
  }

  const isAllowed = allowedOrigins.includes(origin) || origin.endsWith('.vercel.app');
  if (isAllowed) {
    callback(null, true);
  } else {
    // Return callback(null, false) per cors middleware specification instead of throwing an Error
    callback(null, false);
  }
};

const corsOptions = {
  origin: checkOrigin,
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With']
};

app.use(cors(corsOptions));
app.options('*', cors(corsOptions));

app.use(express.json({ limit: '10mb' }));

// Apply Rate Limiters
app.use('/api/', apiLimiter);
app.use('/api/auth/login', authLimiter);
app.use('/api/auth/register', authLimiter);

// Serve static uploads directory for legacy media URL backward compatibility
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// API Routes
app.use('/api/auth', authRoutes);
app.use('/api/users', userRoutes);
app.use('/api/chat', chatRoutes);
app.use('/api/privacy', privacyRoutes);

// Base / Health Check route
app.get('/api/health', (req, res) => {
  res.status(200).json({
    status: 'ok',
    message: 'Messenger Backend Service is running',
    architecture: process.env.DATABASE_URL ? 'Repository Pattern (Supabase PostgreSQL Active)' : 'Repository Pattern (In-Memory Active)',
    databaseConnected: Boolean(process.env.DATABASE_URL),
    storageProvider: 'Supabase Storage Bucket',
    timestamp: new Date().toISOString()
  });
});

// Global Express Error Handler ensuring CORS headers are preserved on error responses
app.use((err, req, res, next) => {
  const origin = req.headers.origin;
  if (origin && (allowedOrigins.includes(origin) || origin.endsWith('.vercel.app'))) {
    res.setHeader('Access-Control-Allow-Origin', origin);
    res.setHeader('Access-Control-Allow-Credentials', 'true');
  }
  console.error('Express Unhandled Error:', err.message);
  res.status(err.status || 500).json({
    message: err.message || 'Internal Server Error'
  });
});

// HTTP Server & Socket.IO Initialization
const server = http.createServer(app);

const io = new Server(server, {
  cors: {
    origin: checkOrigin,
    methods: ['GET', 'POST'],
    credentials: true
  },
  pingTimeout: 60000,
  pingInterval: 25000
});

// Attach Socket.IO authentication & connection handlers
io.use(socketAuthMiddleware);
setupSocketHandlers(io);

const startServer = async () => {
  try {
    if (process.env.DATABASE_URL) {
      await initializeDatabase();

      // Presence is tracked in process-local socket maps. After a crash/restart those
      // maps are empty, so persisted online flags must be reconciled before accepting
      // connections. For multi-instance deployments, use shared presence coordination.
      const stalePresence = await query(
        'UPDATE users SET is_online = FALSE WHERE is_online IS TRUE'
      );
      if (stalePresence.rowCount > 0) {
        console.log(`🧹 Startup presence reconciliation: cleared ${stalePresence.rowCount} stale online flag(s).`);
      }
    }

    await initializeStorageBucket();

    // Start 15-minute periodic server cleanup for 24-hour expired messages
    setInterval(async () => {
      try {
        const count = await ChatService.purgeExpiredMessages();
        if (count > 0) {
          console.log(`🧹 Periodic Cleanup: Purged ${count} expired message(s) (>24h old).`);
        }
      } catch (err) {
        console.error('Error during periodic message purge:', err.message);
      }
    }, 15 * 60 * 1000);

    server.listen(PORT, () => {
      console.log(`=======================================================`);
      console.log(`🚀 Messenger Backend running on port ${PORT}`);
      console.log(`🐘 Database: ${process.env.DATABASE_URL ? 'Supabase PostgreSQL Connected' : 'In-Memory Store'}`);
      console.log(`📦 Storage: Supabase Storage Bucket ('messenger-uploads')`);
      console.log(`🔒 Security: Helmet, Authorization & Rate Limiting active`);
      console.log(`🌐 CORS Allowed Origins: ${allowedOrigins.join(', ')} + *.vercel.app`);
      console.log(`⚡ Socket.IO listening for real-time connections...`);
      console.log(`=======================================================`);
    });
  } catch (err) {
    console.error('❌ Server startup aborted due to initialization failure:', err.message);
    process.exit(1);
  }
};

startServer();
