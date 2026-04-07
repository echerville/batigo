export default async function handler(req, res) {
  if (req.method !== 'POST') return res.status(405).end();

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
    const text = await r.text();
    res.status(r.status).json({ error: text });
  }
}
