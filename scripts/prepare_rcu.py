#!/usr/bin/env python3
"""Réseaux de chaleur France Chaleur Urbaine (déjà reprojetés en WGS84 par ogr2ogr)
→ rcu-lignes.geojsonl (tracés, pour tippecanoe) + rcu-points.json (un point par réseau).
Usage: prepare_rcu.py traces.geojson sans_traces.geojson"""
import json, sys

def num(v):
    try: return float(v)
    except (TypeError, ValueError): return None

def props(p, trace):
    enr, co2 = num(p.get('Taux EnR&R')), num(p.get('contenu CO2'))
    if enr is not None and enr <= 1: enr *= 100   # au cas où le taux serait une fraction
    return {
        'id': p.get('Identifiant reseau') or p.get('Identifian') or '',
        'nom': (p.get('nom_reseau') or 'Réseau de chaleur').strip(),
        'gestionnaire': (p.get('Gestionnaire') or p.get('Gestionnai') or '—').strip(),
        'enr': f'{enr:.1f} %' if enr is not None else '—',
        'co2': f'{co2:.3f} kg/kWh' if co2 is not None else '—',
        'classe': 1 if str(p.get('reseaux classes', '')).lower() in ('1', 'true') else 0,
        'trace': trace,
    }

def centroid(g):
    pts = []
    def walk(c):
        if c and isinstance(c[0], (int, float)): pts.append(c)
        else:
            for x in c: walk(x)
    walk(g['coordinates'])
    return [round(sum(p[0] for p in pts) / len(pts), 6), round(sum(p[1] for p in pts) / len(pts), 6)] if pts else None

traces = json.load(open(sys.argv[1]))['features']
sans = json.load(open(sys.argv[2]))['features']
points = []
with open('rcu-lignes.geojsonl', 'w') as out:
    for f in traces:
        if not f.get('geometry'): continue
        p = props(f['properties'], 1)
        out.write(json.dumps({'type': 'Feature', 'geometry': f['geometry'], 'properties': p}, ensure_ascii=False) + '\n')
        c = centroid(f['geometry'])
        if c: points.append({'type': 'Feature', 'geometry': {'type': 'Point', 'coordinates': c}, 'properties': p})
for f in sans:
    if f.get('geometry'):
        points.append({'type': 'Feature', 'geometry': f['geometry'], 'properties': props(f['properties'], 0)})
json.dump({'type': 'FeatureCollection', 'features': points}, open('rcu-points.json', 'w'), ensure_ascii=False, separators=(',', ':'))
print(f'{len(traces)} tracés, {len(sans)} réseaux sans tracé, {len(points)} points')
print('Exemple :', json.dumps(points[0]['properties'], ensure_ascii=False) if points else '-')
