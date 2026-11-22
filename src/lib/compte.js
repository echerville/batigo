// Comptes Scope (Supabase, Paris). Clé publiable : prévue pour le navigateur, l'accès aux données
// est protégé par les règles de la base (chacun ne lit et n'écrit que ses propres lignes).
import { createClient } from '@supabase/supabase-js';
import { SUPABASE_URL, SUPABASE_KEY } from './config.js';

export const sb = SUPABASE_URL && SUPABASE_KEY
  ? createClient(SUPABASE_URL, SUPABASE_KEY, { auth: { persistSession: true, autoRefreshToken: true, detectSessionInUrl: true } })
  : null;

const RE_MAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
export const mailOk = m => RE_MAIL.test((m || '').trim());

// Envoie le lien de connexion ; le lien ramène sur la page en cours (même domaine)
export async function envoyerLien(email) {
  if (!sb) throw new Error('Comptes indisponibles');
  const { error } = await sb.auth.signInWithOtp({
    email: email.trim(),
    options: { emailRedirectTo: location.origin + location.pathname, shouldCreateUser: true },
  });
  if (error) throw error;
}

export async function deconnecter() { if (sb) await sb.auth.signOut(); }

// Profil : niveau (gratuit ou pro). Créé par la base à l'inscription.
export async function lireProfil(uid) {
  if (!sb || !uid) return null;
  const { data } = await sb.from('profiles').select('niveau, cree_le').eq('id', uid).maybeSingle();
  return data;
}
