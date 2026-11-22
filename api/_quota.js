// Quotas de fiches (étape 3 des comptes). Le préfixe "_" : pas une route publique.
// Compte la fiche dans Supabase (fonction compter_fiche, protégée par QUOTA_SECRET).
// Visiteur sans compte : 3 fiches par jour et par IP ; gratuit : 20 par mois ; Pro : illimité.
const crypto = require('node:crypto');

const URL = process.env.VITE_SUPABASE_URL;
const KEY = process.env.VITE_SUPABASE_KEY;
const SECRET = process.env.QUOTA_SECRET;

function ipHash(req) {
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim() || req.socket?.remoteAddress || '';
  return crypto.createHash('sha256').update(`${SECRET}|${ip}`).digest('hex').slice(0, 32);
}

// Renvoie true si la requête a été refusée (réponse déjà envoyée).
async function quota(req, res) {
  res.setHeader('Cache-Control', 'private, no-store'); // la réponse dépend de la personne
  const cle = String(req.query.fiche || '').slice(0, 120);
  if (!cle) { res.status(400).json({ error: 'fiche manquante' }); return true; }
  if (!URL || !KEY || !SECRET) return false; // comptes non configurés : pas de limite
  const auth = String(req.headers.authorization || '');
  const headers = { apikey: KEY, 'Content-Type': 'application/json' };
  if (/^Bearer\s+\S+/.test(auth)) headers.Authorization = auth;
  try {
    const r = await fetch(`${URL}/rest/v1/rpc/compter_fiche`, {
      method: 'POST', headers, signal: AbortSignal.timeout(5000),
      body: JSON.stringify({ p_secret: SECRET, p_cle: cle, p_ip: ipHash(req) }),
    });
    if (r.status === 401 && headers.Authorization) { // session expirée : on compte comme visiteur
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

module.exports = { quota };
