import folium
import branca

class Popup(object):

    def __init__(self):
        pass
        
    def create_for_point(self, key, locations):
        self._locations = locations
        self._key = key
        popup_html = '''<head><base target="_self"></head><a href="https://duckduckgo.com/?:embed=yes">[Open in DB] </a>Location ''' + str(self._key) + '''<br/>'''
        
        for p in sorted(self._locations[self._key]):
            if not p[0] == '0':
                popup_html += '''ClusterID:{}, Id:{}, Time:{}<br/>'''.format(p[0], p[1], p[2])

        iframe = branca.element.IFrame(html=popup_html, width=500, height=300)
        popup = folium.Popup(iframe, max_width=2650)
        return popup
        
