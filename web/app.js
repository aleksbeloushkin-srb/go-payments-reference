'use strict';

// Friendly copy per status. The chip next to it always shows the exact
// technical status name.
const MOOD = {
  pending: 'Saddling up the ponies…',
  processing: 'Your ponies are on their way',
  requires_action: 'A pony is waiting for your confirmation',
  succeeded: 'Ponies delivered!',
  failed: 'The ponies could not get through',
  canceled: 'The ponies turned back',
};
const FINAL = new Set(['succeeded', 'failed', 'canceled']);
const EXPONENT = { EUR: 2, USD: 2, GBP: 2, CHF: 2, JPY: 0 };

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function newKey() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return 'key-' + Date.now().toString(36) + '-' + Math.random().toString(36).slice(2);
}

// toMinor converts "12.50" to 1250 without floating point.
function toMinor(text, currency) {
  const exp = EXPONENT[currency];
  const m = /^(\d+)(?:[.,](\d+))?$/.exec(text.trim());
  if (!m) throw new Error('Enter an amount like 12.50');
  const frac = m[2] || '';
  if (frac.length > exp) throw new Error(`${currency} has ${exp} decimal places`);
  return Number(m[1] + frac.padEnd(exp, '0'));
}

function formatAmount(minor, currency) {
  const exp = EXPONENT[currency] ?? 2;
  return new Intl.NumberFormat(undefined, {
    style: 'currency', currency, minimumFractionDigits: exp, maximumFractionDigits: exp,
  }).format(minor / 10 ** exp);
}

async function postPayment(body, key) {
  const res = await fetch('/v1/payments', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': key },
    body: JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  return { status: res.status, data, retryAfter: Number(res.headers.get('Retry-After')) || 0 };
}

// postWithRetry repeats the same request, with the same Idempotency-Key, on
// a network error, a 5xx or a 429. It throws if no definite answer arrives.
async function postWithRetry(body, key) {
  let lastError = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const res = await postPayment(body, key);
      if (res.status < 500 && res.status !== 429) return res;
      lastError = new Error(`HTTP ${res.status}`);
      if (attempt < 3) await sleep(Math.max(res.retryAfter * 1000, 1000 * attempt));
    } catch (e) {
      lastError = e;
      if (attempt < 3) await sleep(1000 * attempt);
    }
  }
  throw lastError;
}

// The Idempotency-Key belongs to what the user wants to pay, not to a click.
// A retry after a network error or a lost response reuses it, so the server
// returns the payment it may already have created instead of creating a
// second one. Only a definite answer, or different form values, starts a
// new payment with a new key.
let pending = null; // { intent, key }

function keyFor(req) {
  const intent = JSON.stringify(req);
  if (!pending || pending.intent !== intent) {
    pending = { intent, key: newKey() };
  }
  return pending.key;
}

function setStatus(card, status) {
  const chip = card.querySelector('[data-role="chip"]');
  chip.className = 'chip ' + status + (FINAL.has(status) ? '' : ' live');
  chip.textContent = status;
  card.querySelector('.mood').textContent = MOOD[status] || status;
}

function addTimeline(card, p) {
  const li = document.createElement('li');
  const time = document.createElement('time');
  time.dateTime = p.updated_at || new Date().toISOString();
  time.textContent = new Date(time.dateTime).toLocaleTimeString();
  const chip = document.createElement('span');
  chip.className = 'chip ' + p.status;
  chip.textContent = p.status;
  li.append(time, chip, document.createTextNode(` version ${p.version}`));
  if (p.gateway_ref) li.append(document.createTextNode(` · ${p.gateway_ref}`));
  card.querySelector('.timeline').append(li);
}

// follow long-polls until the payment is final. Each request returns either
// a newer version (modified: true) or, after the soft timeout, the same one.
async function follow(card, id, version) {
  let failures = 0;
  for (;;) {
    let res;
    try {
      res = await fetch(`/v1/payments/${id}/wait?version=${version}`);
    } catch {
      failures++;
      await sleep(Math.min(1000 * 2 ** failures, 15000));
      continue;
    }
    if (res.status === 429 || res.status === 503) {
      const wait = Number(res.headers.get('Retry-After')) || 1;
      await sleep(wait * 1000);
      continue;
    }
    if (!res.ok) {
      failures++;
      card.querySelector('.note').textContent = `Long poll answered ${res.status}, retrying…`;
      await sleep(Math.min(1000 * 2 ** failures, 15000));
      continue;
    }
    failures = 0;
    const p = await res.json();
    if (!p.modified) continue;
    version = p.version;
    setStatus(card, p.status);
    addTimeline(card, p);
    if (FINAL.has(p.status)) return;
  }
}

function addCard(req, key, created) {
  const card = document.getElementById('payment-template').content.firstElementChild.cloneNode(true);
  card.querySelector('.amount').textContent = formatAmount(req.amount_minor, req.currency);
  card.querySelector('.meta').textContent = `to ${req.merchant_id} · payment ${created.id}`;
  setStatus(card, created.status);
  addTimeline(card, created);

  const note = card.querySelector('.note');
  card.querySelector('[data-action="replay"]').addEventListener('click', async () => {
    const { status, data } = await postPayment(req, key);
    note.textContent = `Replay with the same Idempotency-Key: HTTP ${status}, ` +
      (data.id === created.id ? 'same payment id, nothing charged twice.' : `id ${data.id}`);
  });
  card.querySelector('[data-action="conflict"]').addEventListener('click', async () => {
    const { status, data } = await postPayment({ ...req, amount_minor: req.amount_minor + 1 }, key);
    note.textContent = `Same key, different amount: HTTP ${status} ${data.code || ''}`;
  });

  document.getElementById('payments').prepend(card);
  follow(card, created.id, created.version);
}

document.getElementById('pay-form').addEventListener('submit', async (ev) => {
  ev.preventDefault();
  const form = ev.currentTarget;
  const error = document.getElementById('form-error');
  error.textContent = '';
  const currency = form.currency.value;
  let req;
  try {
    req = { merchant_id: form.merchant.value.trim(), amount_minor: toMinor(form.amount.value, currency), currency };
  } catch (e) {
    error.textContent = e.message;
    return;
  }
  const key = keyFor(req);
  const button = form.querySelector('button[type="submit"]');
  button.disabled = true;
  try {
    const { status, data } = await postWithRetry(req, key);
    pending = null; // a definite answer: the next submit is a new payment
    if (status !== 201 && status !== 200) {
      error.textContent = `HTTP ${status}: ${data.message || 'request failed'}`;
      return;
    }
    addCard(req, key, { ...data, updated_at: new Date().toISOString() });
  } catch {
    error.textContent = 'No answer from the server. Press "Send payment" again: the request is ' +
      'repeated with the same Idempotency-Key, so it cannot create a second payment.';
  } finally {
    button.disabled = false;
  }
});
