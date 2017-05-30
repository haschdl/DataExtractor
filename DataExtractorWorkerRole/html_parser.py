import urllib.parse
import json
from bs4 import BeautifulSoup # noqa # pylint: disable=unused-import

def get_querystring(requested_date):
    """Returns the query string for 2nd-nth requests."""     
    f = { 'consulta' : 'rapida', 'periodoInicio' : requested_date, 'periodoFim': requested_date, 'fase':'PAG', 'codigoOS' : 'TOD', 'codigoFavorecido' : ''}
    return urllib.parse.urlencode(f)

def html_table_to_json(soup_object, css_class_name):
    """Given the text of an HTML page, locates the first table element
    of class 'class_name' and parses it into a JSON object.
    """
    
    if soup_object is None:
        return    
    
    # find the section that has the assignment table
    table_from_page = soup_object.find("table", attrs={'class':css_class_name})
    
    # we want the first table in this section, then the tbody inside that
    # and we want all rows in the body
    tablerows = table_from_page.find_all('tr')
    
    result = []
    col = None

    colHeaders = tablerows[0].find_all('th')
    colCount = len(colHeaders)    
    
    for row in tablerows:
        cols = row.find_all('td')
        if cols is None:
            continue

        if len(cols) != colCount:
            continue

        colAppend = {} 
        for x in range(0, colCount):                
            col = cols[x]
            if col:
                if col.a: #content is a element (hyperlink) => exports only the content 
                    colAppend[colHeaders[x].string.strip()] = col.a.string.strip()            
                elif col.contents:
                    colAppend[colHeaders[x].string.strip()] = col.contents[0].string.strip()
                else:
                    colAppend[colHeaders[x].string.strip()] = ''
        result.append(colAppend) 
    
    return json.dumps(result)
