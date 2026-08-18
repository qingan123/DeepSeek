#!/usr/bin/env node
/**
 * DeepSeek PoW 求解器(DeepSeekHashV1)
 * 用法: node solve_pow.js '<challenge-json>'
 * 输出: {"algorithm":"...","challenge":"...","salt":"...","answer":123,"signature":"...","target_path":"..."}
 * 原理: 加载官方 worker 源码(ds-pow-js.js + ds-8138.js),在 vm 沙箱中模拟 Worker 环境,
 *       等待模块 promise 链完成,调用官方 onmessage 求解逻辑,捕获 postMessage 结果。
 */
const fs = require('fs');
const path = require('path');
const vm = require('vm');
const os = require('os');
const { execFile } = require('child_process');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');

const DIR = __dirname;
const NATIVE_SOLVER = process.env.POW_NATIVE_BIN || path.join(DIR, 'dspow_native');
const mainSrc = fs.readFileSync(path.join(DIR, 'ds-pow-js.js'), 'utf8');
const chunkSrc = fs.readFileSync(path.join(DIR, 'ds-8138.js'), 'utf8');

function loadWorkerSandbox(start = 0, step = 1) {
  // 官方 worker 默认执行 i=0; i<difficulty; i++。这里只改变候选数字的
  // 遍历方式，让多个线程分别搜索 start, start+step, ...；Hash算法完全不动。
  const loopNeedle = ',i=0;i<r;i++)if(n.copy().update(String(i)).digest("hex")===t)return i;';
  const loopReplacement = ',i=__powStart;i<r;i+=__powStep)if(n.copy().update(String(i)).digest("hex")===t)return i;';
  if (!mainSrc.includes(loopNeedle)) {
    throw new Error('official PoW loop signature changed; refusing unsafe patch');
  }
  const rangedMainSrc = mainSrc.replace(loopNeedle, loopReplacement);
  const sandbox = {
    console,
    TextEncoder,
    TextDecoder,
    Uint8Array,
    Uint32Array,
    ArrayBuffer,
    DataView,
    SharedArrayBuffer,
    URL,
    navigator: { userAgent: 'node' },
    location: { href: 'https://fe-static.deepseek.com/chat/static/76608.8f2a9fa413.js' },
    setTimeout,
    clearTimeout,
    setInterval,
    clearInterval,
    self: null,
    __powStart: start,
    __powStep: step,
    postMessage: (msg) => { sandbox.__answer = msg; },
    importScripts: () => {
      try { vm.runInContext(chunkSrc, sandbox); } catch (e) { /* 8138 可能重复加载 */ }
    },
  };
  sandbox.self = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(rangedMainSrc, sandbox);
  return sandbox;
}

async function solveRange(challenge, start = 0, step = 1) {
  if (challenge.expireAt === undefined && challenge.expire_at !== undefined) {
    challenge.expireAt = challenge.expire_at;
  }
  const sandbox = loadWorkerSandbox(start, step);
  // 等微任务/promise 链完成(88387 模块注册 onmessage)
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 50));
    const t = vm.runInContext('typeof onmessage', sandbox);
    if (t === 'function') break;
  }
  const r = vm.runInContext('typeof onmessage', sandbox);
  if (r !== 'function') {
    throw new Error('onmessage not available');
  }
  vm.runInContext('onmessage({data:{type:"pow-challenge",challenge:__c}});', Object.assign(sandbox, { __c: challenge }));
  if (!sandbox.__answer) {
    throw new Error('solve failed: worker returned no result');
  }
  if (sandbox.__answer.type === 'pow-error') {
    return null;
  }
  if (sandbox.__answer.type !== 'pow-answer') {
    throw new Error('solve failed: ' + JSON.stringify(sandbox.__answer));
  }
  // pow-answer 消息结构: {type:"pow-answer", answer:{algorithm,challenge,salt,answer:<int>,signature}}
  return sandbox.__answer.answer.answer;
}

function configuredWorkerCount(challenge) {
  const available = typeof os.availableParallelism === 'function'
    ? os.availableParallelism()
    : os.cpus().length;
  const requested = Number.parseInt(process.env.POW_WORKERS || '4', 10);
  const difficulty = Number(challenge.difficulty) || 1;
  return Math.max(1, Math.min(Number.isFinite(requested) ? requested : 4, available, difficulty));
}

async function solveNative(challenge, workerCount) {
  if (process.env.POW_DISABLE_NATIVE === '1') return null;
  if (challenge.algorithm !== 'DeepSeekHashV1') return null;
  if (!fs.existsSync(NATIVE_SOLVER)) return null;
  const expireAt = challenge.expireAt ?? challenge.expire_at;
  if (expireAt === undefined || !challenge.salt || !challenge.challenge) return null;
  const prefix = `${challenge.salt}_${expireAt}_`;

  return await new Promise((resolve, reject) => {
    execFile(
      NATIVE_SOLVER,
      [challenge.challenge, prefix, String(challenge.difficulty), String(workerCount)],
      { timeout: 30000, maxBuffer: 1024 * 1024 },
      (error, stdout, stderr) => {
        if (error) {
          reject(new Error(`native PoW failed: ${stderr || error.message}`));
          return;
        }
        const answer = Number.parseInt(String(stdout).trim(), 10);
        if (!Number.isSafeInteger(answer) || answer < 0 || answer >= Number(challenge.difficulty)) {
          reject(new Error(`native PoW returned invalid answer: ${String(stdout).trim()}`));
          return;
        }
        resolve(answer);
      },
    );
  });
}

async function solve(challenge) {
  if (challenge.expireAt === undefined && challenge.expire_at !== undefined) {
    challenge = { ...challenge, expireAt: challenge.expire_at };
  }
  const workerCount = configuredWorkerCount(challenge);
  try {
    const nativeAnswer = await solveNative(challenge, workerCount);
    if (Number.isInteger(nativeAnswer)) return nativeAnswer;
  } catch (error) {
    // 原生程序缺失、异常或上游算法变化时，自动退回已验证的官方JS实现。
    if (process.env.POW_NATIVE_REQUIRED === '1') throw error;
    console.error(`WARN: ${error.message}; falling back to official JS solver`);
  }
  if (workerCount === 1) {
    const answer = await solveRange(challenge, 0, 1);
    if (!Number.isInteger(answer)) throw new Error('No solution found');
    return answer;
  }

  return await new Promise((resolve, reject) => {
    const workers = [];
    let finished = 0;
    let settled = false;

    const stopAll = () => {
      for (const worker of workers) worker.terminate().catch(() => {});
    };
    const fail = (error) => {
      if (settled) return;
      settled = true;
      stopAll();
      reject(error instanceof Error ? error : new Error(String(error)));
    };

    for (let start = 0; start < workerCount; start++) {
      const worker = new Worker(__filename, {
        workerData: { challenge, start, step: workerCount },
      });
      workers.push(worker);
      worker.once('message', (message) => {
        if (settled) return;
        if (message && Number.isInteger(message.answer)) {
          settled = true;
          stopAll();
          resolve(message.answer);
          return;
        }
        if (message && message.error) {
          fail(new Error(message.error));
          return;
        }
        finished += 1;
        if (finished === workerCount) fail(new Error('No solution found'));
      });
      worker.once('error', fail);
      worker.once('exit', (code) => {
        if (!settled && code !== 0) fail(new Error(`PoW worker exited with code ${code}`));
      });
    }
  });
}

if (!isMainThread) {
  solveRange(workerData.challenge, workerData.start, workerData.step)
    .then((answer) => parentPort.postMessage({ answer }))
    .catch((error) => parentPort.postMessage({ error: error.message }));
}

if (require.main === module && isMainThread) {
  const input = process.argv[2];
  if (!input) {
    console.error('usage: node solve_pow.js <challenge-json>');
    process.exit(2);
  }
  solve(JSON.parse(input))
    .then((answer) => {
      const challenge = JSON.parse(input);
      const result = {
        algorithm: challenge.algorithm,
        challenge: challenge.challenge,
        salt: challenge.salt,
        answer: answer,
        signature: challenge.signature,
        target_path: challenge.target_path,
      };
      console.log(JSON.stringify(result));
    })
    .catch((e) => {
      console.error('ERROR: ' + e.message);
      process.exit(1);
    });
}

module.exports = { solve };
