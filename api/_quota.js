// Quotas de fiches (étape 3 des comptes). Le préfixe "_" : pas une route publique.
// Compte la fiche dans Supabase (fonction compter_fiche, protégée par QUOTA_SECRET).
// Visiteur sans compte : N fiches par jour et par IP ; gratuit : M par mois ; Pro : illimité.
// N et M sont réglables dans la base (prive.reglages : limite_anonyme_jour, limite_gratuit_mois).
const crypto = require('node:crypto');

const URL = process.env.VITE_SUPABASE_URL;
const KEY = process.env.VITE_SUPABASE_KEY;
const SECRET = process.env.QUOTA_SECRET;

// Cookie « humain » signé : date d'expiration + signature HMAC
function signerHumain(exp) {
  const sig = crypto.createHmac('sha256', `${SECRET}|humain`).update(String(exp)).digest('base64url');
  return `${exp}.${sig}`;
}
function estHumain(req) {
  if (!process.env.TURNSTILE_SECRET) return true; // captcha non configuré
  const m = String(req.headers.cookie || '').match(/(?:^|;\s*)scope_h=(\d+)\.([\w-]+)/);
  if (!m || Number(m[1]) < Date.now()) return false;
  const attendu = signerHumain(m[1]).split('.')[1];
  return attendu.length === m[2].length && crypto.timingSafeEqual(Buffer.from(attendu), Buffer.from(m[2]));
}

function ipHash(req) {
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket?.remoteAddress || '';
  return crypto.createHash('sha256').update(`${SECRET}|${ip}`).digest('hex').slice(0, 32);
}

// Appel à la base. compter = false : vérification seule.
async function appel(req, compter) {
  const auth = String(req.headers.authorization || '');
  const headers = { apikey: KEY, 'Content-Type': 'application/json' };
  if (/^Bearer\s+\S+/.test(auth)) headers.Authorization = auth;
  const cle = String(req.query.fiche || '').slice(0, 120);
  const r = await fetch(`${URL}/rest/v1/rpc/compter_fiche`, {
    method: 'POST', headers, signal: AbortSignal.timeout(5000),
    body: JSON.stringify({ p_secret: SECRET, p_cle: cle, p_ip: ipHash(req), p_compter: compter }),
  });
  return r;
}

// Compte la fiche une fois qu'un bâtiment est identifié (erreurs ignorées : on ne bloque pas Scope)
async function compter(req) {
  if (!URL || !KEY || !SECRET) return;
  try { await appel(req, true); } catch { /* rien */ }
}

// Vérifie avant de chercher le bâtiment. Renvoie true si la requête a été refusée (réponse déjà envoyée).
async function quota(req, res) {
  res.setHeader('Cache-Control', 'private, no-store'); // la réponse dépend de la personne
  const cle = String(req.query.fiche || '').slice(0, 120);
  if (!cle) { res.status(400).json({ error: 'fiche manquante' }); return true; }
  if (!URL || !KEY || !SECRET) return false; // comptes non configurés : pas de limite
  const hasAuth = /^Bearer\s+\S+/.test(String(req.headers.authorization || ''));
  // Visiteur sans compte : vérification Turnstile (cookie valable 7 jours) avant toute fiche
  if (!hasAuth && !estHumain(req)) { res.status(403).json({ error: 'captcha' }); return true; }
  try {
    const r = await appel(req, false);
    if (r.status === 401 && hasAuth) { // jeton expiré : le navigateur rafraîchit sa session et réessaie
      res.status(401).json({ error: 'session expirée' }); return true;
    }
    if (!r.ok) return false; // service de comptes indisponible : on ne bloque pas Scope
    const q = await r.json();
    res.setHeader('X-Scope-Niveau', q.niveau || '');
    if (q.ok === false) {
      res.status(402).json({ error: 'limite', niveau: q.niveau, limite: q.limite, utilise: q.utilise });
      return true;
    }
    return false;
  } catch {
    return false;
  }
}

module.exports = { quota, compter, signerHumain };
