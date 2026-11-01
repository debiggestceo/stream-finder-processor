const PROCESSOR_URL = "https://stream-finder-processor.onrender.com";
const GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions";
const GROQ_CHAT_MODEL = "openai/gpt-oss-120b";

async function telegram(env, chatId, text) {
  const message = String(text).slice(0, 4000);
  return fetch(`https://api.telegram.org/bot${env.TELEGRAM_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: chatId, text: message })
  });
}

function detectPlatform(url) {
  try {
    const host = new URL(url).hostname.toLowerCase().replace(/^www\./, "");
    if (host === "kick.com") return "kick";
    if (host === "twitch.tv" || host.endsWith(".twitch.tv")) return "twitch";
    if (host === "youtube.com" || host.endsWith(".youtube.com") || host === "youtu.be") return "youtube";
  } catch {}
  return "unknown";
}

function extractStreamUrl(text) {
  const match = String(text).match(/https?:\/\/[^\s]+/i);
  if (!match) return null;
  const url = match[0].replace(/[),.!?]+$/, "");
  return ["kick", "twitch", "youtube"].includes(detectPlatform(url)) ? url : null;
}

async function dispatchChunk(env, job) {
  await env.DB.prepare(`
    UPDATE stream_job_state SET status = 'running', updated_at = CURRENT_TIMESTAMP WHERE job_id = ?
  `).bind(job.job_id).run();

  const response = await fetch(`${PROCESSOR_URL}/process-chunk`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "X-Processor-Secret": env.PROCESSOR_SECRET },
    body: JSON.stringify({
      url: job.url, chat_id: job.chat_id, job_id: job.job_id, chunk_index: job.next_chunk,
      platform: job.platform || detectPlatform(job.url)
    })
  });

  if (!response.ok) {
    const body = await response.text();
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'processing', updated_at = datetime('now', '-11 minutes'), error = ? WHERE job_id = ?
    `).bind(body.slice(0, 1000), job.job_id).run();
    throw new Error(`Render error ${response.status}: ${body}`);
  }
}

async function startJob(env, chatId, url) {
  const jobId = crypto.randomUUID();
  const platform = detectPlatform(url);

  await env.DB.prepare(`
    INSERT INTO streams (chat_id, url, platform, status, job_id)
    VALUES (?, ?, ?, 'processing', ?)
  `).bind(String(chatId), url, platform, jobId).run();

  await env.DB.prepare(`
    INSERT INTO stream_job_state (job_id, chat_id, url, next_chunk, status)
    VALUES (?, ?, ?, 0, 'processing')
  `).bind(jobId, String(chatId), url).run();

  const job = { job_id: jobId, chat_id: String(chatId), url, platform, next_chunk: 0 };
  await dispatchChunk(env, job);

  await telegram(env, chatId, "Принял запись! Начинаю обработку и расшифровку. Сообщу, когда всё будет готово.");
}

async function processorChunk(request, env) {
  if (!env.PROCESSOR_SECRET || request.headers.get("X-Processor-Secret") !== env.PROCESSOR_SECRET) {
    return new Response("Unauthorized", { status: 401 });
  }

  const data = await request.json();
  const jobId = String(data.job_id || "");
  const chatId = String(data.chat_id || "");
  const chunkIndex = Number(data.chunk_index);
  const status = data.status;

  if (!jobId || !chatId || !Number.isInteger(chunkIndex) || chunkIndex < 0) {
    return Response.json({ error: "Invalid callback" }, { status: 400 });
  }

  const job = await env.DB.prepare(`SELECT * FROM stream_job_state WHERE job_id = ?`).bind(jobId).first();
  if (!job) return Response.json({ error: "Job not found" }, { status: 404 });

  if (chunkIndex !== job.next_chunk) {
    return Response.json({ status: "ignored", expected_chunk: job.next_chunk });
  }

  if (status === "error") {
    const error = String(data.error || "Unknown processing error");
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'processing', updated_at = datetime('now', '-11 minutes'), error = ? WHERE job_id = ?
    `).bind(error.slice(0, 1000), jobId).run();

    await telegram(env, chatId, `Ошибка при обработке фрагмента ${chunkIndex + 1}. Попробую повторить.`);
    return Response.json({ status: "retry_scheduled" });
  }

  if (status === "completed") {
    await env.DB.prepare(`
      UPDATE stream_job_state SET status = 'completed', updated_at = CURRENT_TIMESTAMP, error = NULL WHERE job_id = ?
    `).bind(jobId).run();

    const rows = await env.DB.prepare(`
      SELECT start_time, end_time, transcript FROM stream_segments WHERE job_id = ? ORDER BY segment_index
    `).bind(jobId).all();

    const segments = (rows.results || []).map(row => ({ time: row.start_time, end_time: row.end_time, text: row.transcript }));
    const transcript = segments.map(s => `[${s.time}] ${s.text}`).join("\n");

    await env.DB.prepare(`
      UPDATE streams SET status = 'completed', transcript = ?, error = NULL WHERE job_id = ?
    `).bind(transcript, jobId).run();

    await telegram(env, chatId, "Расшифровка готова! Обработана вся запись.\n\nТеперь можешь задавать вопросы по стриму.");
    return Response.json({ status: "completed" });
  }

  if (status !== "chunk_completed") return Response.json({ error: "Unknown status" }, { status: 400 });

  const segments = Array.isArray(data.segments) ? data.segments : [];
  const statements = segments.map((segment, i) => env.DB.prepare(`
    INSERT OR IGNORE INTO stream_segments (job_id, chat_id, segment_index, start_time, end_time, transcript)
    VALUES (?, ?, ?, ?, ?, ?)
  `).bind(
    jobId, chatId, chunkIndex * 10000 + i,
    String(segment.time || "00:00:00"),
    formatTime(Number(segment.end || segment.start || 0)),
    String(segment.text || "")
  ));
  if (statements.length) await env.DB.batch(statements);

  await env.DB.prepare(`
    UPDATE stream_job_state SET next_chunk = ?, status = 'processing', updated_at = CURRENT_TIMESTAMP, error = NULL WHERE job_id = ?
  `).bind(chunkIndex + 1, jobId).run();

  try {
    await dispatchChunk(env, { job_id: jobId, chat_id: chatId, url: job.url, platform: detectPlatform(job.url), next_chunk: chunkIndex + 1 });
  } catch (error) {
    console.error("Next chunk dispatch failed:", error);
  }

  return Response.json({ status: "saved", saved_segments: segments.length, next_chunk: chunkIndex + 1 });
}

function formatTime(seconds) {
  seconds = Math.max(0, Math.floor(seconds));
  return [Math.floor(seconds / 3600), Math.floor((seconds % 3600) / 60), seconds % 60]
    .map(v => String(v).padStart(2, "0")).join(":");
}

function normalizeText(text) {
  return String(text || "").toLowerCase().replace(/ё/g, "е").replace(/[^\p{L}\p{N}\s]/gu, " ").replace(/\s+/g, " ").trim();
}

const STOP_WORDS = new Set(["а","и","но","или","в","на","по","с","со","к","у","из","за","от","до","не","что","это","он","она","они","его","ее","их","мы","вы","я","ты","ли","бы","же","ну","вот","там","тут","как","про","мне","меня","него","нее","the","and","for","you","that","this","with"]);

async function expandSearchTerms(env, question) {
  if (!env.GROQ_API_KEY) return [question];
  try {
    const response = await fetch(GROQ_CHAT_URL, {
      method: "POST",
      headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: GROQ_CHAT_MODEL, temperature: 0,
        messages: [
          { role: "system", content: "Верни только JSON-массив коротких поисковых фраз на русском языке. Добавь разговорные варианты и синонимы. Максимум 12 элементов." },
          { role: "user", content: question }
        ]
      })
    });
    if (!response.ok) return [question];
    const result = await response.json();
    const match = (result.choices?.[0]?.message?.content || "").match(/\[[\s\S]*\]/);
    if (!match) return [question];
    const terms = JSON.parse(match[0]);
    return [question, ...terms].filter(x => typeof x === "string").slice(0, 13);
  } catch (error) {
    console.error("Search expansion failed:", error);
    return [question];
  }
}

async function handleQuestion(env, chatId, question) {
  const stream = await env.DB.prepare(`
    SELECT job_id, status FROM streams WHERE chat_id = ? ORDER BY id DESC LIMIT 1
  `).bind(String(chatId)).first();

  if (!stream) return telegram(env, chatId, "Сначала отправь ссылку на запись Kick, Twitch или YouTube.");
  if (stream.status !== "completed") return telegram(env, chatId, "Запись ещё обрабатывается. Задай вопрос после завершения.");

  const terms = await expandSearchTerms(env, question);
  const normalizedTerms = terms.map(normalizeText).filter(Boolean);
  const words = normalizeText(question).split(" ").filter(w => w.length > 2 && !STOP_WORDS.has(w));

  const rows = await env.DB.prepare(`
    SELECT start_time, end_time, transcript FROM stream_segments WHERE job_id = ? ORDER BY segment_index
  `).bind(stream.job_id).all();

  const segments = rows.results || [];
  if (!segments.length) return telegram(env, chatId, "В этой записи пока нет сохранённой расшифровки.");

  const ranked = segments.map(segment => {
    const text = normalizeText(segment.transcript);
    let score = 0;
    for (const term of normalizedTerms) {
      const normalized = normalizeText(term);
      if (normalized.length > 2 && text.includes(normalized)) score += 3;
    }
    for (const word of words) if (text.includes(word)) score += 1;
    return { ...segment, score };
  }).filter(s => s.score > 0).sort((a,b) => b.score - a.score).slice(0, 50);

  if (!env.GROQ_API_KEY) {
    if (!ranked.length) return telegram(env, chatId, "Поиск не нашёл совпадений в расшифровке.");
    return telegram(env, chatId, ranked.slice(0, 10).map(s => `[${s.start_time}] ${s.transcript}`).join("\n"));
  }

  const context = ranked.length ? ranked.map(s => `[${s.start_time}] ${s.transcript}`).join("\n") : "Поиск по расшифровке не нашёл совпадений.";

  try {
    const response = await fetch(GROQ_CHAT_URL, {
      method: "POST",
      headers: { "Authorization": `Bearer ${env.GROQ_API_KEY}`, "Content-Type": "application/json" },
      body: JSON.stringify({
        model: GROQ_CHAT_MODEL, temperature: 0,
        messages: [
          { role: "system", content: "Ты анализируешь расшифровку стрима. Отвечай по-русски и кратко. Используй только предоставленные фрагменты. Не выдумывай факты и таймкоды. Если доказательств нет, скажи, что по найденным фрагментам подтвердить это нельзя. Если вопрос предполагает да/нет, сначала дай ответ, затем примеры с таймкодами." },
          { role: "user", content: `Вопрос: ${question}\n\nФрагменты расшифровки:\n${context}` }
        ]
      })
    });
    if (!response.ok) throw new Error(await response.text());
    const result = await response.json();
    return telegram(env, chatId, result.choices?.[0]?.message?.content || "Не удалось сформировать ответ.");
  } catch (error) {
    console.error("Groq answer error:", error);
    return telegram(env, chatId, "Не удалось обработать вопрос. Попробуй ещё раз.");
  }
}

async function handleTelegram(update, env) {
  const message = update.message;
  if (!message?.text || !message.chat?.id) return;

  const chatId = message.chat.id;
  const text = message.text.trim();

  if (text === "/start" || text === "/help") {
    return telegram(env, chatId,
      "Привет! Я Stream Finder.\n\nОтправь ссылку на запись Kick, Twitch или YouTube, и я расшифрую её.\n\nПосле обработки задавай вопросы по содержанию записи."
    );
  }

  const streamUrl = extractStreamUrl(text);
  if (streamUrl) {
    try {
      await startJob(env, chatId, streamUrl);
    } catch (error) {
      console.error("Start job error:", error);
      await telegram(env, chatId, "Не удалось запустить обработку: " + String(error).slice(0, 500));
    }
    return;
  }

  if (text.startsWith("/")) return telegram(env, chatId, "Неизвестная команда. Используй /start.");
  return handleQuestion(env, chatId, text);
}

async function recoverJobs(env) {
  const result = await env.DB.prepare(`
    SELECT job_id, chat_id, url, next_chunk FROM stream_job_state
    WHERE status IN ('processing', 'running') AND updated_at < datetime('now', '-10 minutes')
    LIMIT 5
  `).all();

  for (const job of result.results || []) {
    try { await dispatchChunk(env, { ...job, platform: detectPlatform(job.url) }); }
    catch (error) { console.error("Recovery failed:", job.job_id, error); }
  }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/") {
      return Response.json({ status: "Stream Finder Worker online" });
    }

    if (request.method === "POST" && url.pathname === "/processor-chunk") {
      try { return await processorChunk(request, env); }
      catch (error) {
        console.error("Processor callback error:", error);
        return Response.json({ error: String(error) }, { status: 500 });
      }
    }

    if (request.method === "POST") {
      try {
        await handleTelegram(await request.json(), env);
        return Response.json({ ok: true });
      } catch (error) {
        console.error("Telegram handler error:", error);
        return Response.json({ ok: false }, { status: 500 });
      }
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(recoverJobs(env));
  }
};
