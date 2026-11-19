// Garde-fous communs aux fonctions api/.
// Le préfixe "_" empêche Vercel d'en faire une route publique.

const crypto = require('node:crypto');

// Limite de requêtes par IP. Mémoire propre à chaque instance Vercel :
// c'est un frein, pas une garantie. La vraie limite viendra avec les comptes (étape 3).
const WINDOW_MS = 60_000;
const MAX_PER_WINDOW = 60;
const hits = new Map();

function clientIp(req) {
  const fwd = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  return fwd || req.socket?.remoteAddress || 'inconnu';
}

function isRateLimited(req) {
  const ip = clientIp(req);
  const now = Date.now();
  const entry = hits.get(ip);
  if (!entry || now - entry.start > WINDOW_MS) {
    if (hits.size > 5000) hits.clear();
    hits.set(ip, { start: now, count: 1 });
    return false;
  }
  entry.count += 1;
  return entry.count > MAX_PER_WINDOW;
}

// Renvoie true si la requête a été refusée (la réponse est alors déjà envoyée).
// Plus d'en-tête "Access-Control-Allow-Origin: *" : seul le site lui-même
// (appels relatifs /api/...) peut utiliser ces fonctions depuis un navigateur.
function guard(req, res, methods = ['GET']) {
  if (!methods.includes(req.method)) {
    res.setHeader('Allow', methods.join(', '));
    res.status(405).json({ error: 'Méthode non autorisée' });
    return true;
  }
  if (req.headers['sec-fetch-site'] === 'cross-site') {
    res.status(403).json({ error: 'Origine non autorisée' });
    return true;
  }
  if (isRateLimited(req)) {
    res.setHeader('Retry-After', '60');
    res.status(429).json({ error: 'Trop de requêtes, réessayez dans une minute' });
    return true;
  }
  return false;
}

// Comparaison à temps constant (évite de deviner un secret caractère par caractère).
function sameSecret(given, expected) {
  const a = Buffer.from(String(given || ''));
  const b = Buffer.from(String(expected || ''));
  return a.length === b.length && b.length > 0 && crypto.timingSafeEqual(a, b);
}

module.exports = { guard, sameSecret };
