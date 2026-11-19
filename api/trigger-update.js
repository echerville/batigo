// Lance la régénération des tuiles DPE (GitHub Action).
// Protégé par un jeton : variable d'environnement Vercel ADMIN_TOKEN,
// envoyé par le navigateur dans l'en-tête x-admin-token.
const { guard, sameSecret } = require('./_guard');

module.exports = async function handler(req, res) {
  if (guard(req, res, ['POST'])) return;

  const adminToken = process.env.ADMIN_TOKEN;
  if (!adminToken) return res.status(503).json({ error: 'ADMIN_TOKEN non configuré' });
  if (!sameSecret(req.headers['x-admin-token'], adminToken)) {
    return res.status(401).json({ error: 'Non autorisé' });
  }

  const token = process.env.GITHUB_PAT;
  if (!token) return res.status(500).json({ error: 'GITHUB_PAT non configuré' });

  const r = await fetch(
    'https://api.github.com/repos/echerville/batigo/actions/workflows/generate-dpe-tiles.yml/dispatches',
    {
      method: 'POST',
      headers: {
        Authorization: `token ${token}`,
        Accept: 'application/vnd.github.v3+json',
        'Content-Type': 'application/json',
        'User-Agent': 'scope-app'
      },
      body: JSON.stringify({ ref: 'main' })
    }
  );

  if (r.status === 204) {
    res.status(200).json({ ok: true });
  } else {
    // Pas de détail GitHub renvoyé au navigateur
    res.status(502).json({ error: `GitHub a répondu ${r.status}` });
  }
};
