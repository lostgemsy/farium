import express from "express";
import session from "express-session";
import bcrypt from "bcryptjs";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { hostname } from "node:os";
import { Server } from "socket.io";
import { createClient } from "redis";
import { epoxyPath } from "@mercuryworkshop/epoxy-transport";
import { libcurlPath } from "@mercuryworkshop/libcurl-transport";
import { baremuxPath } from "@mercuryworkshop/bare-mux/node";
import { bareModulePath } from "@mercuryworkshop/bare-as-module3";
import { server as wisp } from "@mercuryworkshop/wisp-js/server";
import venus from "venus-pit";

const __dirname = process.cwd();
const app = express();
const publicPath = join(__dirname, "public");
const DATA_DIR = join(__dirname, "data");
const DATA_FILE = join(DATA_DIR, "store.json");
const CHAT_PORT = Number(process.env.CHAT_PORT || 3001);
const CHAT_SOCKET_URL = process.env.CHAT_SOCKET_URL || `http://localhost:${CHAT_PORT}`;
const REDIS_URL = process.env.REDIS_URL || process.env.diddya_REDIS_URL || null;

const userSockets = new Map();
let redisClient = null;
let redisSub = null;
let redisReady = false;

async function ensureDataDir() {
  await mkdir(DATA_DIR, { recursive: true });

  if (!existsSync(DATA_FILE)) {
    await writeFile(
      DATA_FILE,
      JSON.stringify(
        {
          users: [],
          messages: [],
        },
        null,
        2,
      ),
      "utf8",
    );
  }
}

async function loadStore() {
  await ensureDataDir();

  const raw = await readFile(DATA_FILE, "utf8");
  try {
    return JSON.parse(raw);
  } catch {
    return { users: [], messages: [] };
  }
}

async function saveStore(store) {
  await ensureDataDir();
  await writeFile(DATA_FILE, JSON.stringify(store, null, 2), "utf8");
}

function normalizeUsername(username) {
  return String(username || "").trim();
}

function findUserByName(store, username) {
  const name = normalizeUsername(username).toLowerCase();
  return store.users.find((user) => user.username.toLowerCase() === name) || null;
}

function getSafeUser(user) {
  if (!user) return null;

  return {
    username: user.username,
    role: user.role,
    banned: Boolean(user.banned),
    muted: Boolean(user.muted),
  };
}

function listUsers(store) {
  return store.users
    .filter((user) => user && user.username)
    .map((user) => ({
      username: user.username,
      role: user.role,
      banned: Boolean(user.banned),
      muted: Boolean(user.muted),
    }));
}

async function initRedis() {
  if (!REDIS_URL) {
    console.log("Redis not configured; using local JSON store for chat data.");
    return;
  }

  try {
    redisClient = createClient({ url: REDIS_URL });
    redisSub = createClient({ url: REDIS_URL });

    await Promise.all([redisClient.connect(), redisSub.connect()]);
    redisReady = true;

    await redisSub.subscribe("farium:messages:channel");
    redisSub.on("message", (channel, message) => {
      if (channel !== "farium:messages:channel") return;

      try {
        const parsed = JSON.parse(message);
        globalThis.__io?.emit("chat:message", parsed);
      } catch (error) {
        console.error("Failed to parse Redis chat message:", error);
      }
    });

    console.log("Redis connected for chat storage and presence.");
  } catch (error) {
    redisReady = false;
    console.warn("Redis unavailable; falling back to file store.", error.message || error);
  }
}

async function saveMessageRedis(message) {
  if (!redisReady || !redisClient) return false;

  await redisClient.lPush("farium:messages", JSON.stringify(message));
  await redisClient.lTrim("farium:messages", 0, 249);
  await redisClient.publish("farium:messages:channel", JSON.stringify(message));
  return true;
}

async function getRecentMessagesRedis(limit = 100) {
  if (!redisReady || !redisClient) return [];

  const list = await redisClient.lRange("farium:messages", 0, Math.max(limit - 1, 0));
  return list
    .map((entry) => {
      try {
        return JSON.parse(entry);
      } catch {
        return null;
      }
    })
    .filter(Boolean)
    .reverse();
}

async function setPresenceRedis(username, socketId) {
  if (!redisReady || !redisClient) return;

  await redisClient.hSet("farium:presence", username, JSON.stringify({
    socketId,
    lastSeen: new Date().toISOString(),
  }));
}

async function clearPresenceRedis(username) {
  if (!redisReady || !redisClient) return;

  await redisClient.hDel("farium:presence", username);
}

async function listPresenceRedis() {
  if (!redisReady || !redisClient) return [];

  const all = await redisClient.hGetAll("farium:presence");
  return Object.entries(all).map(([username, value]) => {
    try {
      const parsed = JSON.parse(value);
      return {
        username,
        socketId: parsed.socketId,
        lastSeen: parsed.lastSeen,
      };
    } catch {
      return { username, socketId: null, lastSeen: null };
    }
  });
}

async function appendChatMessage(message) {
  if (redisReady && redisClient) {
    await saveMessageRedis(message);
    return;
  }

  const store = await loadStore();
  store.messages.push(message);
  if (store.messages.length > 250) {
    store.messages = store.messages.slice(-250);
  }
  await saveStore(store);
}

async function getRecentMessages(limit = 100) {
  if (redisReady && redisClient) {
    return getRecentMessagesRedis(limit);
  }

  const store = await loadStore();
  return store.messages.slice(-limit);
}

async function ensureSeedAdmin() {
  const store = await loadStore();
  const existing = findUserByName(store, "hohogames");

  if (!existing) {
    store.users.push({
      username: "hohogames",
      passwordHash: bcrypt.hashSync(process.env.ADMIN_PASSWORD || "ChangeMe123!", 10),
      role: "admin",
      banned: false,
      muted: false,
    });
    await saveStore(store);
  }
}

async function requireAuthMiddleware(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.redirect("/login");
  }

  const store = await loadStore();
  const user = findUserByName(store, req.session.user.username);

  if (!user || user.banned) {
    req.session.destroy(() => {
      res.redirect("/login");
    });
    return;
  }

  req.session.user = getSafeUser(user);
  next();
}

async function requireAdminMiddleware(req, res, next) {
  if (!req.session || !req.session.user) {
    return res.redirect("/login");
  }

  const store = await loadStore();
  const user = findUserByName(store, req.session.user.username);

  if (!user || user.banned || user.role !== "admin") {
    req.session.destroy(() => {
      res.redirect("/login");
    });
    return;
  }

  req.session.user = getSafeUser(user);
  next();
}

async function broadcastPresence() {
  const store = await loadStore();

  if (redisReady && redisClient) {
    const presenceEntries = await listPresenceRedis();
    const userMap = new Map(store.users.map((user) => [user.username, user]));
    const users = presenceEntries
      .map((entry) => {
        const user = userMap.get(entry.username) || null;
        if (!user) return null;
        return {
          username: user.username,
          role: user.role,
          banned: Boolean(user.banned),
          muted: Boolean(user.muted),
        };
      })
      .filter(Boolean)
      .filter((user) => !user.banned);

    globalThis.__io?.emit("presence:update", users);
    return;
  }

  const users = listUsers(store)
    .filter((user) => !user.banned)
    .map((user) => ({
      username: user.username,
      role: user.role,
      muted: user.muted,
    }));

  globalThis.__io?.emit("presence:update", users);
}

app.use(express.urlencoded({ extended: true }));
app.use(express.json());
app.use(
  session({
    secret: process.env.SESSION_SECRET || "farium-chat-secret",
    resave: false,
    saveUninitialized: false,
    cookie: {
      httpOnly: true,
      secure: false,
      maxAge: 1000 * 60 * 60 * 24 * 7,
    },
  }),
);

const server = createServer();
const chatServer = createServer();
const io = new Server(chatServer, {
  cors: {
    origin: "*",
  },
});

globalThis.__io = io;

chatServer.listen(CHAT_PORT, () => {
  console.log(`Chat socket server listening on http://localhost:${CHAT_PORT}`);
});

await initRedis();

io.on("connection", async (socket) => {
  const username = normalizeUsername(socket.handshake.auth?.username || "");

  if (!username) {
    socket.emit("auth:error", "Missing username");
    socket.disconnect();
    return;
  }

  const store = await loadStore();
  const user = findUserByName(store, username);

  if (!user || user.banned) {
    socket.emit("auth:error", "Banned or invalid user");
    socket.disconnect();
    return;
  }

  socket.username = username;
  socket.join(`user:${username}`);
  userSockets.set(username, socket.id);

  if (redisReady && redisClient) {
    await setPresenceRedis(username, socket.id);
  }

  const history = await getRecentMessages(100);
  socket.emit("chat:history", history);

  await broadcastPresence();

  socket.on("chat:send", async ({ text }) => {
    const messageText = String(text || "").trim();
    if (!messageText) return;

    const currentStore = await loadStore();
    const currentUser = findUserByName(currentStore, username);

    if (!currentUser || currentUser.banned) {
      socket.emit("chat:error", "You are banned and cannot chat.");
      return;
    }

    if (currentUser.muted) {
      socket.emit("chat:error", "You are muted and cannot send messages.");
      return;
    }

    const message = {
      id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
      username: currentUser.username,
      text: messageText.slice(0, 500),
      createdAt: new Date().toISOString(),
    };

    await appendChatMessage(message);

    if (!(redisReady && redisClient)) {
      io.emit("chat:message", message);
    }
  });

  socket.on("call:offer", ({ to, offer }) => {
    const targetSocketId = userSockets.get(normalizeUsername(to));
    if (!targetSocketId) {
      socket.emit("call:error", { message: "User is offline" });
      return;
    }

    io.to(targetSocketId).emit("call:incoming", {
      from: username,
      offer,
    });
  });

  socket.on("call:answer", ({ to, answer }) => {
    const targetSocketId = userSockets.get(normalizeUsername(to));
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:answer", { from: username, answer });
    }
  });

  socket.on("call:ice", ({ to, candidate }) => {
    const targetSocketId = userSockets.get(normalizeUsername(to));
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:ice", { from: username, candidate });
    }
  });

  socket.on("call:hangup", ({ to }) => {
    const targetSocketId = userSockets.get(normalizeUsername(to));
    if (targetSocketId) {
      io.to(targetSocketId).emit("call:hangup", { from: username });
    }
  });

  socket.on("disconnect", async () => {
    if (userSockets.get(username) === socket.id) {
      userSockets.delete(username);
    }

    if (redisReady && redisClient) {
      await clearPresenceRedis(username);
    }

    await broadcastPresence();
  });
});

app.use(express.static(publicPath));

app.get("/background.png", (req, res) => {
  const candidates = [
    join(__dirname, "background.png"),
    join(publicPath, "background.png"),
  ];

  for (const filePath of candidates) {
    if (existsSync(filePath)) {
      return res.sendFile(filePath);
    }
  }

  return res.status(404).send("background.png not found");
});

app.set("view engine", "ejs");
app.set("views", join(__dirname, "views"));

const __venus = venus(app);

app.get("/robots.txt", (req, res) => {
  res.type("text/plain");
  res.send(`User-agent: *\nDisallow: ${__venus}`);
});

app.get("/login", (req, res) => {
  res.render("login");
});

app.get("/register", (req, res) => {
  res.render("register");
});

app.get("/logout", (req, res) => {
  req.session.destroy(() => {
    res.redirect("/login");
  });
});

app.get("/chat", requireAuthMiddleware, async (req, res) => {
  const store = await loadStore();
  const user = findUserByName(store, req.session.user.username);
  if (!user || user.banned) {
    return res.redirect("/login");
  }

  res.render("chat", {
    user: getSafeUser(user),
    socketUrl: CHAT_SOCKET_URL,
  });
});

app.get("/admin", requireAdminMiddleware, async (req, res) => {
  const store = await loadStore();
  const user = findUserByName(store, req.session.user.username);
  if (!user || user.role !== "admin") {
    return res.redirect("/login");
  }

  res.render("admin", {
    user: getSafeUser(user),
  });
});

app.get("/api/session", (req, res) => {
  res.json({ user: req.session.user || null });
});

app.get("/api/messages", requireAuthMiddleware, async (req, res) => {
  const messages = await getRecentMessages(100);
  res.json({ messages });
});

app.get("/api/users", requireAuthMiddleware, async (req, res) => {
  const store = await loadStore();
  res.json({ users: listUsers(store).filter((user) => !user.banned) });
});

app.post("/api/register", async (req, res) => {
  const username = normalizeUsername(req.body.username);
  const password = String(req.body.password || "");

  if (!username || username.length < 3 || password.length < 4) {
    return res.status(400).json({ error: "Username must be at least 3 chars and password at least 4 chars." });
  }

  const store = await loadStore();
  const existing = findUserByName(store, username);

  if (existing) {
    return res.status(409).json({ error: "User already exists." });
  }

  store.users.push({
    username,
    passwordHash: bcrypt.hashSync(password, 10),
    role: "user",
    banned: false,
    muted: false,
  });

  await saveStore(store);
  await broadcastPresence();
  res.json({ success: true, username });
});

app.post("/api/login", async (req, res) => {
  const username = normalizeUsername(req.body.username);
  const password = String(req.body.password || "");

  if (!username || !password) {
    return res.status(400).json({ error: "Missing username or password." });
  }

  const store = await loadStore();
  const user = findUserByName(store, username);

  if (!user || user.banned) {
    return res.status(401).json({ error: "Invalid credentials or account banned." });
  }

  const valid = bcrypt.compareSync(password, user.passwordHash);
  if (!valid) {
    return res.status(401).json({ error: "Invalid credentials." });
  }

  req.session.user = getSafeUser(user);
  res.json({ success: true, user: req.session.user });
});

app.get("/api/admin/users", requireAdminMiddleware, async (req, res) => {
  const store = await loadStore();
  res.json({ users: listUsers(store) });
});

app.post("/api/admin/ban", requireAdminMiddleware, async (req, res) => {
  const username = normalizeUsername(req.body.username);
  if (!username) return res.status(400).json({ error: "Username is required" });

  const store = await loadStore();
  const user = findUserByName(store, username);
  if (!user) return res.status(404).json({ error: "User not found" });

  user.banned = true;
  await saveStore(store);
  await broadcastPresence();
  res.json({ success: true, user: getSafeUser(user) });
});

app.post("/api/admin/unban", requireAdminMiddleware, async (req, res) => {
  const username = normalizeUsername(req.body.username);
  if (!username) return res.status(400).json({ error: "Username is required" });

  const store = await loadStore();
  const user = findUserByName(store, username);
  if (!user) return res.status(404).json({ error: "User not found" });

  user.banned = false;
  await saveStore(store);
  await broadcastPresence();
  res.json({ success: true, user: getSafeUser(user) });
});

app.post("/api/admin/mute", requireAdminMiddleware, async (req, res) => {
  const username = normalizeUsername(req.body.username);
  if (!username) return res.status(400).json({ error: "Username is required" });

  const store = await loadStore();
  const user = findUserByName(store, username);
  if (!user) return res.status(404).json({ error: "User not found" });

  user.muted = true;
  await saveStore(store);
  await broadcastPresence();
  res.json({ success: true, user: getSafeUser(user) });
});

app.post("/api/admin/unmute", requireAdminMiddleware, async (req, res) => {
  const username = normalizeUsername(req.body.username);
  if (!username) return res.status(400).json({ error: "Username is required" });

  const store = await loadStore();
  const user = findUserByName(store, username);
  if (!user) return res.status(404).json({ error: "User not found" });

  user.muted = false;
  await saveStore(store);
  await broadcastPresence();
  res.json({ success: true, user: getSafeUser(user) });
});

app.get("/", async (req, res) => {
  await ensureSeedAdmin();

  const renderWithInjectedChatButton = (err, html) => {
    if (err) {
      console.error(err);
      return res.status(500).send("Failed to render homepage.");
    }

    const injection = `
      <script>
        (() => {
          function ensureChatButton() {
            if (document.getElementById('farium-chat-btn')) return;
            const btn = document.createElement('a');
            btn.id = 'farium-chat-btn';
            btn.href = '/chat';
            btn.textContent = 'Chat';
            btn.style.position = 'fixed';
            btn.style.bottom = '24px';
            btn.style.right = '24px';
            btn.style.zIndex = '9999';
            btn.style.padding = '12px 20px';
            btn.style.borderRadius = '999px';
            btn.style.background = 'linear-gradient(135deg, #5cb85c, #3a9e3a)';
            btn.style.color = '#fff';
            btn.style.fontWeight = '700';
            btn.style.textDecoration = 'none';
            btn.style.boxShadow = '0 18px 38px -18px rgba(92, 184, 92, 0.9)';
            btn.style.fontFamily = 'Inter, sans-serif';
            document.body.appendChild(btn);
          }
          window.addEventListener('load', ensureChatButton);
        })();
      </script>
    `;

    const withButton = html.includes("</body>")
      ? html.replace("</body>", `${injection}</body>`)
      : `${html}${injection}`;

    res.send(withButton);
  };

  res.render("index", { _venus: __venus }, renderWithInjectedChatButton);
});

app.get("/settings", requireAuthMiddleware, (req, res) => {
  res.render("settings/index");
});

app.get("/settings/styles", requireAuthMiddleware, (req, res) => {
  res.render("settings/styles");
});

app.get("/settings/misc", requireAuthMiddleware, (req, res) => {
  res.render("settings/misc");
});

app.get("/search", requireAuthMiddleware, (req, res) => {
  res.render("search");
});

server.on("request", (req, res) => {
  res.setHeader("Cross-Origin-Opener-Policy", "same-origin");
  res.setHeader("Cross-Origin-Embedder-Policy", "anonymous");
  app(req, res);
});

server.on("upgrade", (req, socket, head) => {
  wisp.routeRequest(req, socket, head);
});

let port = parseInt(process.env.PORT || "3000", 10);
if (Number.isNaN(port)) port = 80;

server.on("listening", () => {
  const address = server.address();
  console.log("Listening on:");
  console.log(`\thttp://localhost:${address.port}`);
  console.log(`\thttp://${hostname()}:${address.port}`);
  console.log(`\thttp://${address.family === "IPv6" ? `[${address.address}]` : address.address}:${address.port}`);
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

function shutdown() {
  console.log("SIGTERM signal received: closing HTTP server");
  server.close();
  chatServer.close();
  process.exit(0);
}

server.listen({ port });
await ensureSeedAdmin();

