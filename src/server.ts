import express from 'express';
import { createServer } from 'http';
import { Server, Socket } from 'socket.io';
import cors from 'cors';

const app = express();
app.use(cors());

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).send({ status: 'ok', uptime: process.uptime() });
});

const httpServer = createServer(app);
const io = new Server(httpServer, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST']
  }
});

interface QueueItem {
  songId: string;
  title: string;
  addedBy: string;
}

interface RoomState {
  roomId: string;
  hostId: string | null;
  isPlaying: boolean;
  currentTime: number;
  songId: string;
  lastUpdated: number; // Date.now() when last updated
  queue: QueueItem[];
}

// Global in-memory store for room state
const rooms: Map<string, RoomState> = new Map();

// Map to track which room and username a socket belongs to
const socketMetadata: Map<string, { roomId: string; username: string }> = new Map();

io.on('connection', (socket: Socket) => {
  console.log(`[Server] Client connected: ${socket.id}`);

  // Handle room joining
  socket.on('join-room', (data: { roomId: string; username: string }) => {
    const { roomId, username } = data;
    socket.join(roomId);
    socketMetadata.set(socket.id, { roomId, username });

    console.log(`[Server] User '${username}' (${socket.id}) joined room: ${roomId}`);

    let room = rooms.get(roomId);
    if (!room) {
      // Create new room if it doesn't exist, and assign this client as host
      room = {
        roomId,
        hostId: socket.id,
        isPlaying: false,
        currentTime: 0,
        songId: '',
        lastUpdated: Date.now(),
        queue: []
      };
      rooms.set(roomId, room);
      console.log(`[Server] Room created: ${roomId}. Host assigned: ${socket.id} (${username})`);
    }

    // Send the current room state back to the newly joined client
    let currentCalculatedTime = room.currentTime;
    if (room.isPlaying && room.lastUpdated > 0) {
      // Basic latency compensation: estimate current position based on time elapsed since last host update
      const elapsedSeconds = (Date.now() - room.lastUpdated) / 1000;
      currentCalculatedTime += elapsedSeconds;
    }

    socket.emit('room-state', {
      roomId: room.roomId,
      hostId: room.hostId,
      isPlaying: room.isPlaying,
      currentTime: currentCalculatedTime,
      songId: room.songId,
      isHost: room.hostId === socket.id,
      queue: room.queue
    });

    // Notify others in the room
    socket.to(roomId).emit('user-joined', {
      socketId: socket.id,
      username,
      hostId: room.hostId
    });

    // Broadcast updated member list to room
    sendRoomMembers(roomId);
  });

  // Handle host state changes or local events
  socket.on('playback-command', (data: {
    command: 'play' | 'pause' | 'seek' | 'song-change';
    currentTime: number;
    songId: string;
    senderTimestamp: number;
  }) => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;

    const { roomId, username } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const isHost = room.hostId === socket.id || room.hostId === null;
    if (!isHost) {
      // Reject command and send back correct state
      console.log(`[Server] Unauthorized playback command rejected from non-host client: ${username}`);
      socket.emit('force-sync', {
        isPlaying: room.isPlaying,
        currentTime: room.isPlaying ? room.currentTime + (Date.now() - room.lastUpdated) / 1000 : room.currentTime,
        songId: room.songId
      });
      return;
    }

    // Update room state
    room.isPlaying = data.command === 'play' || data.command === 'song-change' || (data.command === 'seek' ? room.isPlaying : false);
    if (data.command === 'pause') {
      room.isPlaying = false;
    }
    room.currentTime = data.currentTime;
    room.songId = data.songId;
    room.lastUpdated = Date.now();

    console.log(`[Server] Playback command from Host (${username}) in room ${roomId}: ${data.command} at ${data.currentTime}s (Song ID: ${data.songId})`);

    // Broadcast command to other clients in the room
    socket.to(roomId).emit('remote-playback-command', {
      command: data.command,
      currentTime: data.currentTime,
      songId: data.songId,
      serverTimestamp: Date.now()
    });
  });

  // Allow clients to request a manual sync from the host
  socket.on('request-sync', () => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;
    const { roomId } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    let currentCalculatedTime = room.currentTime;
    if (room.isPlaying && room.lastUpdated > 0) {
      const elapsedSeconds = (Date.now() - room.lastUpdated) / 1000;
      currentCalculatedTime += elapsedSeconds;
    }

    socket.emit('room-state', {
      roomId: room.roomId,
      hostId: room.hostId,
      isPlaying: room.isPlaying,
      currentTime: currentCalculatedTime,
      songId: room.songId,
      isHost: room.hostId === socket.id,
      queue: room.queue
    });
  });

  // Handle claiming host role
  socket.on('claim-host', (data?: { songId: string; currentTime: number; isPlaying: boolean }) => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;
    const { roomId, username } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    room.hostId = socket.id;
    
    // Update room state with new host state
    if (data && data.songId) {
      room.songId = data.songId;
      room.currentTime = data.currentTime;
      room.isPlaying = data.isPlaying;
      console.log(`[Server] User ${username} claimed host role. Syncing room to: Song ID ${room.songId} at ${room.currentTime}s (playing: ${room.isPlaying})`);
    } else {
      console.log(`[Server] User ${username} claimed host role.`);
    }

    room.lastUpdated = Date.now();

    io.in(roomId).emit('host-changed', {
      hostId: socket.id,
      hostUsername: username
    });

    // Broadcast state to other clients immediately
    if (data && data.songId) {
      socket.to(roomId).emit('remote-playback-command', {
        command: room.isPlaying ? 'play' : 'pause',
        currentTime: room.currentTime,
        songId: room.songId,
        serverTimestamp: Date.now()
      });
    }

    sendRoomMembers(roomId);
  });

  // -------------------------------------------------------------
  // Queue Sync Events
  // -------------------------------------------------------------

  // Add a song to the queue
  socket.on('add-to-queue', (data: { songId: string; title: string }) => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;
    const { roomId, username } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    const newItem: QueueItem = {
      songId: data.songId,
      title: data.title || 'Canción Desconocida',
      addedBy: username
    };

    room.queue.push(newItem);
    console.log(`[Server] Song added to queue in room ${roomId}: "${newItem.title}" by ${username}`);

    // Broadcast updated queue to the room
    io.in(roomId).emit('queue-updated', room.queue);
  });

  // Remove a song from the queue (by index)
  socket.on('remove-from-queue', (data: { index: number }) => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;
    const { roomId, username } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    if (data.index >= 0 && data.index < room.queue.length) {
      const removed = room.queue.splice(data.index, 1);
      console.log(`[Server] Song removed from queue in room ${roomId}: "${removed[0].title}" by ${username}`);
      io.in(roomId).emit('queue-updated', room.queue);
    }
  });

  // Autoplay next song from queue (invoked by the Host)
  socket.on('play-next-from-queue', () => {
    const meta = socketMetadata.get(socket.id);
    if (!meta) return;
    const { roomId, username } = meta;
    const room = rooms.get(roomId);
    if (!room) return;

    // Verify sender is host
    if (room.hostId !== socket.id) return;

    if (room.queue.length > 0) {
      const nextSong = room.queue.shift(); // Remove first element
      if (nextSong) {
        room.songId = nextSong.songId;
        room.currentTime = 0;
        room.isPlaying = true;
        room.lastUpdated = Date.now();

        console.log(`[Server] Autoplaying next song from queue in room ${roomId}: "${nextSong.title}"`);

        // Broadcast queue update and song change
        io.in(roomId).emit('queue-updated', room.queue);
        io.in(roomId).emit('remote-playback-command', {
          command: 'song-change',
          currentTime: 0,
          songId: room.songId,
          serverTimestamp: Date.now()
        });
      }
    }
  });

  // Handle ping
  socket.on('ping-sync', (clientTime: number) => {
    socket.emit('pong-sync', {
      clientTime,
      serverTime: Date.now()
    });
  });

  // Handle explicit leaving
  socket.on('leave-room', () => {
    handleUserLeaving(socket);
  });

  // Disconnection cleanup
  socket.on('disconnect', () => {
    console.log(`[Server] Client disconnected: ${socket.id}`);
    handleUserLeaving(socket);
  });
});

function handleUserLeaving(socket: Socket) {
  const meta = socketMetadata.get(socket.id);
  if (!meta) return;

  const { roomId, username } = meta;
  socketMetadata.delete(socket.id);
  socket.leave(roomId);

  const room = rooms.get(roomId);
  if (room) {
    // If host leaves, designate new host
    if (room.hostId === socket.id) {
      const clientsInRoom = io.sockets.adapter.rooms.get(roomId);
      if (clientsInRoom && clientsInRoom.size > 0) {
        const newHostId = Array.from(clientsInRoom)[0];
        room.hostId = newHostId;
        const newHostMeta = socketMetadata.get(newHostId);
        const newHostUsername = newHostMeta ? newHostMeta.username : 'Guest';

        console.log(`[Server] Host left. New host designated: ${newHostId} (${newHostUsername})`);
        io.in(roomId).emit('host-changed', {
          hostId: newHostId,
          hostUsername: newHostUsername
        });
      } else {
        rooms.delete(roomId);
        console.log(`[Server] Room empty. Room deleted: ${roomId}`);
      }
    } else {
      socket.to(roomId).emit('user-left', {
        socketId: socket.id,
        username
      });
    }

    sendRoomMembers(roomId);
  }
}

// Helper to gather and send room members
function sendRoomMembers(roomId: string) {
  const room = rooms.get(roomId);
  if (!room) return;

  const clientsInRoom = io.sockets.adapter.rooms.get(roomId);
  const members: Array<{ socketId: string; username: string; isHost: boolean }> = [];

  if (clientsInRoom) {
    clientsInRoom.forEach((socketId) => {
      const meta = socketMetadata.get(socketId);
      if (meta) {
        members.push({
          socketId,
          username: meta.username,
          isHost: room.hostId === socketId
        });
      }
    });
  }

  io.in(roomId).emit('room-members', members);
}

// Start Server
const PORT = process.env.PORT || 3000;
httpServer.listen(PORT, () => {
  console.log(`[Server] SongwFriends Sync Server listening on port ${PORT}`);
});
