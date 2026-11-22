// Vérification « humain » des visiteurs sans compte (Cloudflare Turnstile).
// Reçoit le jeton du widget, le fait valider par Cloudflare, puis pose un cookie signé valable 7 jours.
const { guard } = require('./_guard');
const { signerHumain } = require('./_quota');

module.exports = async function handler(req, res) {
  if (guard(req, res, ['POST'])) return;
  const secret = process.env.TURNSTILE_SECRET;
  if (!secret) return res.status(204).end();
  let token = '';
  try { token = String((typeof req.body === 'string' ? JSON.parse(req.body) : req.body)?.token || ''); } catch { /* vide */ }
  if (!token || token.length > 4096) return res.status(400).json({ error: 'jeton manquant' });
  const ip = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  try {
    const form = new URLSearchParams({ secret, response: token });
    if (ip) form.set('remoteip', ip);
    const r = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', { method: 'POST', body: form, signal: AbortSignal.timeout(8000) });
    const v = await r.json();
    if (!v.success) return res.status(403).json({ error: 'vérification refusée' });
    const jours = 7;
    res.setHeader('Set-Cookie', `scope_h=${signerHumain(Date.now() + jours * 864e5)}; Path=/; Max-Age=${jours * 86400}; HttpOnly; Secure; SameSite=Lax`);
    res.setHeader('Cache-Control', 'no-store');
    return res.status(204).end();
  } catch {
    return res.status(502).json({ error: 'service de vérification indisponible' });
  }
};
