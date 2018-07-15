import csv
import folium
import branca
import datetime, time
from math import sin, cos, sqrt, atan2, radians, degrees
from folium.plugins import MarkerCluster
from folium.plugins import MeasureControl
import popup

import smallest_enclosing_circle as sec

#csv_path = 'eps25_time30_20000rec.csv'
csv_path = '../output_0.003eps_3600min_1500rows.csv/part-00000-d78f9394-7c1b-4ef5-87a0-5963c98d63ad.csv'
output_html_file_name = 'visualization3.html'
R = 6373.0

def calculate_radius_in_meters(x, y, r):
    lat1 = radians(x)
    lon1 = radians(y)
    lat2 = radians(x)
    lon2 = radians(y + r)

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c * 1000.0


colors = ['red', 'blue', 'green', 'purple', 'orange', 'darkred',
             'lightred', 'beige', 'darkblue', 'darkgreen', 'cadetblue',
             'darkpurple', 'lightgreen', 'gray', 'black']
locations = {}
clusters = {}
with open(csv_path, newline='\n', encoding='utf-8') as csvfile:    
    rows = csv.DictReader(csvfile, delimiter=',', quotechar='"')
    counter = 0
    for row in rows:
        print(row['timestamp'])
        date = datetime.datetime.fromtimestamp(int(row['timestamp'])//1000.0)
        l = (float(row['longitude']), float(row['latitude']))
        p = (row['clusterId'], row['id'], date)
        
        if l in locations:
            locations[l].append(p)
        else:
            locations[l] = [p]
            
        if row['clusterId'] in clusters:
            clusters[row['clusterId']]['points'].append(l)
        else:
            clusters[row['clusterId']] = {}
            clusters[row['clusterId']]['points'] = [l]
            clusters[row['clusterId']]['color'] = colors[counter % len(colors)]
            counter += 1
        

m = folium.Map(location=[55.797043,49.06307], zoom_start=22)
#m = folium.Map(location=[50.453472500000004, 30.530381499999997], zoom_start=22)

m.add_child(folium.LatLngPopup())
m.add_child(MeasureControl())
m.add_tile_layer()
circles_fg = folium.map.FeatureGroup(name='hide/show cluster circles').add_to(m)

legend_html = '''
                <div style="position: fixed; 
                            bottom: 10px; left: 10px; width: 210px; height: 80px; 
                            border:2px solid grey; z-index:9999; font-size:14px;
                            ">&nbsp; Legend <br>
                              &nbsp; <i class="fa fa-map-marker fa-2x" style="color:green"></i> Cluster point (different colors)&nbsp; <br>
                              &nbsp; <i class="fa-rotate-0 fa fa-object-group  icon-white"></i> Multiple clusters in one point &nbsp;
                </div>
                '''
m.get_root().html.add_child(folium.Element(legend_html))


popup_builder = popup.Popup()
created_points = set()
point_count = 0
for counter, clusterId in enumerate(sorted(clusters.keys())):
    # skip points not in cluster
    if int(clusterId) == 0:
        continue
    # cut most of the dataset
    #if counter == 10: break
        
    points_in_cluster = len(clusters[clusterId]['points'])
    cluster_fg = folium.map.FeatureGroup(name='[{}] {} cluster'.format(points_in_cluster, str(clusterId))).add_to(m)
    clusters[clusterId]['feature_group'] = cluster_fg
    
    if points_in_cluster > 1 and not clusterId == '0':
        if points_in_cluster == 2:
            lat1 = clusters[clusterId]['points'][0][0]
            lon1 = clusters[clusterId]['points'][0][1]
            lat2 = clusters[clusterId]['points'][1][0]
            lon2 = clusters[clusterId]['points'][1][1]
            
            lx = lat2 - lat1
            ly = lon2 - lon1
            
            radius = sqrt(lx**2 + ly**2) / 2.0
            x = (lat1 + lat2)/2.0
            y = (lon1 + lon2)/2.0
        else:
            (x, y, radius) = sec.make_circle(clusters[clusterId]['points'])
        distance = calculate_radius_in_meters(x, y, radius) * 2
        print('Cluster', clusterId, 'center', x, y, distance)

        points_ids = []
        for l in sorted(clusters[clusterId]['points']):
            for p in locations[l]:
                points_ids.append("ids="+p[1])
        
        iframe = folium.IFrame(html='''<a href="http://127.0.0.1:8080/searchIds/?{}">[Open in DB] </a>Cluster {} [{}] {}<br>'''.format("&".join(points_ids), str(clusterId), points_in_cluster, clusters[clusterId]['color']), width=500, height=300)
        p = folium.Popup(iframe, max_width=2650)
        folium.features.Circle(
            radius=distance,
            location=[x, y],
            popup=p,
            color=clusters[clusterId]['color'],
            fill=True,
        ).add_to(circles_fg)

    location_list = []
    popups = []
    icons = []
    for l in clusters[clusterId]['points']:
        if l in created_points:
            continue
        else:
            created_points.add(l)

        is_same_cluster = True
        for p in sorted(locations[l]):
            if not p[0] == '0':
                if p[0] != clusterId:
                    is_same_cluster = False
                point_count += 1

        popup = popup_builder.create_for_point(l, locations)
        if is_same_cluster:
            location_list.append(l)
            popups.append(popup)
            icons.append(folium.Icon(prefix='fa', color=clusters[clusterId]['color'], icon='circle'))
        else:
            folium.Marker(list(l), popup=popup, icon=folium.Icon(color=clusters[clusterId]['color'], icon='object-group', prefix='fa')).add_to(m)
    
    MarkerCluster(locations=location_list, popups=popups, icons=icons).add_to(clusters[clusterId]['feature_group'])

folium.LayerControl().add_to(m)
m.save(output_html_file_name)
print(point_count)
