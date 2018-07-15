import folium
import branca

class Popup(object):

    def __init__(self):
        pass
        
    def create_for_point(self, key, locations):
        self._locations = locations
        self._key = key

        points_ids = []
        for p in sorted(self._locations[self._key]):
            points_ids.append("ids="+p[1])
        
        popup_html = '''<a href="http://127.0.0.1:8080/searchIds/?''' + "&".join(points_ids) + '''">[Open all in DB] </a>Location ''' + str(self._key) + '''<br/><body/>'''
        
        for p in sorted(self._locations[self._key]):
            if not p[0] == '0':
                popup_html += '''<a href="http://127.0.0.1:8080/searchIds/?ids={}">[Open in DB] </a>ClusterID:{}, Id:{}, Time:{}<br/>'''.format(p[1], p[0], p[1], p[2])

        iframe = branca.element.IFrame(html=popup_html, width=500, height=300)
        popup = folium.Popup(iframe, max_width=2650)
        return popup
        
