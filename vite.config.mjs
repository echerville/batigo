import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Les fichiers de static/ sont copiés tels quels à la racine du site :
// static/public/… reste servi sous /public/…, static/classic.html sous /classic.html.
export default defineConfig({
  plugins: [react()],
  publicDir: 'static',
  // CSS non minifié : le minifieur supprimait backdrop-filter non préfixé (plus de flou dans Chrome)
  build: { outDir: 'dist', sourcemap: false, chunkSizeWarningLimit: 1500, cssMinify: false },
});
