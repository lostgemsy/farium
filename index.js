import express from "express";
import session from "express-session";
import bcrypt from "bcryptjs";
import { createServer } from "node:http";
import { existsSync } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { hostname } from "node:os";
import { Server } from "socket.io";
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

const userSockets = new Map();

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
  const users = listUsers(store)
    .filter((user) => !user.banned)
    .map((user) => ({ username: user.username, role: user.role, muted: user.muted }));

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

  socket.emit("chat:history", store.messages.slice(-100));
  socket.emit("presence:update", listUsers(store).filter((item) => !item.banned));

  broadcastPresence();

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

    currentStore.messages.push(message);
    if (currentStore.messages.length > 250) {
      currentStore.messages = currentStore.messages.slice(-250);
    }

    await saveStore(currentStore);
    io.emit("chat:message", message);
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

  socket.on("disconnect", () => {
    if (userSockets.get(username) === socket.id) {
      userSockets.delete(username);
    }
    broadcastPresence();
  });
});

app.use(express.static(publicPath));

app.get("/background.png", (req, res) => {
  res.type("image/png");
  res.sendFile(join(__dirname, "background.png"));
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
  const store = await loadStore();
  res.json({ messages: store.messages.slice(-100) });
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
  const store = await loadStore();
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
