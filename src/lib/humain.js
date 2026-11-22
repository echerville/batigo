// Vérification Cloudflare Turnstile pour les visiteurs sans compte.
// Le widget est le plus souvent invisible ; il ne s'affiche (en bas de l'écran) que si Cloudflare a un doute.
const SITEKEY = import.meta.env.VITE_TURNSTILE_SITEKEY || '';
let script = null, enCours = null;

function chargerScript() {
  if (window.turnstile) return Promise.resolve();
  if (!script) {
    script = new Promise((ok, ko) => {
      const s = document.createElement('script');
      s.src = 'https://challenges.cloudflare.com/turnstile/v0/api.js?render=explicit';
      s.async = true; s.onload = ok; s.onerror = () => { script = null; ko(new Error('Turnstile indisponible')); };
      document.head.appendChild(s);
    });
  }
  return script;
}

export function verifierHumain() {
  if (!SITEKEY) return Promise.resolve(false);
  if (enCours) return enCours;
  enCours = (async () => {
    await chargerScript();
    const boite = document.createElement('div');
    boite.className = 'turnstile-boite';
    document.body.appendChild(boite);
    try {
      const jeton = await new Promise((ok, ko) => {
        window.turnstile.render(boite, {
          sitekey: SITEKEY, appearance: 'interaction-only', language: 'fr',
          callback: ok, 'error-callback': () => ko(new Error('échec')), 'timeout-callback': () => ko(new Error('délai')),
        });
      });
      const r = await fetch('/api/humain', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ token: jeton }) });
      return r.ok;
    } finally {
      boite.remove();
      enCours = null;
    }
  })();
  return enCours;
}
