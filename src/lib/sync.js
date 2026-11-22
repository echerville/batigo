// Synchronisation des cartes privées (Mes repères, Mes DPE) avec le compte.
// Le navigateur garde une copie locale ; le serveur fait foi une fois la première synchronisation faite.
import { sb } from './compte.js';

const TABLES = {
  reperes: { table: 'reperes', key: 'id' },
  mesdpe: { table: 'mes_dpe', key: 'n' },
};

// Lit toutes les lignes de l'utilisateur (les règles de la base ne renvoient que les siennes)
export async function lireTout(kind) {
  const { table, key } = TABLES[kind];
  const out = [];
  for (let from = 0; ; from += 1000) {
    const { data, error } = await sb.from(table).select(`${key}, data`).range(from, from + 999);
    if (error) throw error;
    out.push(...data.map(r => r.data));
    if (data.length < 1000) break;
  }
  return out;
}

// Première synchronisation : on garde tout ce qui est d'un côté ou de l'autre ;
// en cas de doublon, la version du serveur l'emporte.
export function fusionner(local, serveur, key) {
  const m = new Map(local.map(x => [x[key], x]));
  for (const x of serveur) m.set(x[key], x);
  return [...m.values()];
}

// Envoie la différence entre l'état connu du serveur (snapshot) et la liste actuelle
export async function envoyerDiff(kind, snapshot, liste) {
  const { table, key } = TABLES[kind];
  const now = new Map(liste.map(x => [x[key], JSON.stringify(x)]));
  const aEcrire = [], aSuppr = [];
  for (const [k, v] of now) if (snapshot.get(k) !== v) aEcrire.push({ [key]: k, data: JSON.parse(v), maj: new Date().toISOString() });
  for (const k of snapshot.keys()) if (!now.has(k)) aSuppr.push(k);
  for (let i = 0; i < aEcrire.length; i += 500) {
    const { error } = await sb.from(table).upsert(aEcrire.slice(i, i + 500), { onConflict: `user_id,${key}` });
    if (error) throw error;
  }
  for (let i = 0; i < aSuppr.length; i += 200) {
    const { error } = await sb.from(table).delete().in(key, aSuppr.slice(i, i + 200));
    if (error) throw error;
  }
  return now;
}

export const snapshotDe = (liste, key) => new Map(liste.map(x => [x[key], JSON.stringify(x)]));
