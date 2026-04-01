#!/usr/bin/env python3
"""
Extrait les bâtiments avec DPE du ZIP BDNB Loire-Atlantique → GeoJSON WGS84.
Usage: python3 extract_dpe_44.py bdnb_dep44.zip dpe-44.geojson
"""
import sys
import zipfile
import json
import io
import pandas as pd
from pyproj import Transformer
from shapely import wkt as shapely_wkt
from shapely.ops import transform as shapely_transform
from shapely.geometry import mapping

TRANSFORMER = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)

COLS_NEEDED = ['batiment_groupe_id', 'classe_bilan_dpe', 'geom_groupe',
               'nb_log', 'usage_principal_bdnb_open']


def find_target_csv(zip_path):
    """Trouve le CSV qui contient geom_groupe + classe_bilan_dpe."""
    with zipfile.ZipFile(zip_path) as z:
        names = z.namelist()
        print(f"Fichiers dans le ZIP ({len(names)} total):", file=sys.stderr)
        csv_files = [n for n in names if n.lower().endswith('.csv')]
        for n in csv_files:
            print(f"  {n}", file=sys.stderr)

        # Priorité : batiment_groupe_complet
        for candidate in csv_files:
            base = candidate.lower()
            if 'batiment_groupe_complet' in base:
                # Vérifier que les colonnes existent
                with z.open(candidate) as f:
                    header = f.readline().decode('utf-8', errors='replace').strip()
                    cols = [c.strip('"') for c in header.split(',')]
                    if ';' in header:
                        cols = [c.strip('"') for c in header.split(';')]
                    print(f"Colonnes de {candidate}: {cols[:10]}...", file=sys.stderr)
                    if 'classe_bilan_dpe' in cols and 'geom_groupe' in cols:
                        print(f"→ Utilisation de: {candidate}", file=sys.stderr)
                        return candidate, ';' if ';' in header else ','

        # Fallback : chercher n'importe quel CSV avec les deux colonnes
        for candidate in csv_files:
            with z.open(candidate) as f:
                header = f.readline().decode('utf-8', errors='replace').strip()
                sep = ';' if header.count(';') > header.count(',') else ','
                cols = [c.strip('"') for c in header.split(sep)]
                if 'classe_bilan_dpe' in cols and 'geom_groupe' in cols:
                    print(f"→ Fallback vers: {candidate}", file=sys.stderr)
                    return candidate, sep

    print("ERREUR: Aucun CSV avec geom_groupe + classe_bilan_dpe trouvé", file=sys.stderr)
    sys.exit(1)


def convert_geom(wkt_str):
    """Convertit WKT Lambert-93 → GeoJSON WGS84."""
    if not wkt_str or str(wkt_str).strip() in ('', 'nan', 'None'):
        return None
    try:
        geom = shapely_wkt.loads(str(wkt_str))
        # Vérifier si déjà en WGS84 (coordonnées < 180)
        bounds = geom.bounds
        if abs(bounds[0]) > 180 or abs(bounds[2]) > 180:
            # Lambert-93 → WGS84
            geom = shapely_transform(TRANSFORMER.transform, geom)
        return mapping(geom)
    except Exception as e:
        return None


def main(zip_path, output_path):
    print(f"Ouverture de {zip_path}...", file=sys.stderr)
    csv_file, sep = find_target_csv(zip_path)

    # Colonnes disponibles
    with zipfile.ZipFile(zip_path) as z:
        with z.open(csv_file) as f:
            header_line = f.readline().decode('utf-8', errors='replace').strip()
    all_cols = [c.strip('"').strip() for c in header_line.split(sep)]
    usecols = [c for c in COLS_NEEDED if c in all_cols]
    print(f"Colonnes utilisées: {usecols}", file=sys.stderr)

    # Lecture en chunks pour économiser la RAM
    features = []
    chunk_size = 50_000
    total_rows = 0

    with zipfile.ZipFile(zip_path) as z:
        with z.open(csv_file) as raw:
            text_f = io.TextIOWrapper(raw, encoding='utf-8', errors='replace')
            reader = pd.read_csv(
                text_f,
                sep=sep,
                usecols=usecols,
                dtype=str,
                chunksize=chunk_size,
                low_memory=False,
            )
            for chunk in reader:
                total_rows += len(chunk)
                # Filtrer les lignes avec DPE
                mask = chunk['classe_bilan_dpe'].notna() & (chunk['classe_bilan_dpe'].str.strip() != '')
                filtered = chunk[mask]

                for _, row in filtered.iterrows():
                    geom = convert_geom(row.get('geom_groupe'))
                    if geom is None:
                        continue
                    features.append({
                        'type': 'Feature',
                        'geometry': geom,
                        'properties': {
                            'id':     str(row.get('batiment_groupe_id', '') or ''),
                            'dpe':    str(row.get('classe_bilan_dpe', '') or '').strip(),
                            'nb_log': int(float(row.get('nb_log', 0) or 0)) if str(row.get('nb_log', '')).replace('.','').isdigit() else 0,
                            'usage':  str(row.get('usage_principal_bdnb_open', '') or ''),
                        }
                    })

                print(f"  Lignes traitées: {total_rows}, features DPE: {len(features)}", file=sys.stderr)

    print(f"\nTotal lignes: {total_rows}", file=sys.stderr)
    print(f"Bâtiments avec DPE: {len(features)}", file=sys.stderr)

    geojson = {'type': 'FeatureCollection', 'features': features}
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(geojson, f, ensure_ascii=False)
    print(f"GeoJSON écrit dans {output_path}", file=sys.stderr)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python3 extract_dpe_44.py <input.zip> <output.geojson>")
        sys.exit(1)
    main(sys.argv[1], sys.argv[2])
