import requests
uri = 'http://localhost/icd/release/11/2024-01/mms'
base_uri = 'http://localhost/icd/release/11/2024-01/mms/{}'
# HTTP header fields to set
headers = {
           'Accept': 'application/json', 
           'Accept-Language': 'en',
	   'API-Version': 'v2'}

def retrieve_code(uri):
    req = requests.get(uri, headers=headers, verify=False)
    output = req.json()		

    # classKinds are chapter, block, window
    if 'parent' in output:
        # this has a parent
        if output['classKind'] == 'category' and 'child' not in output:
	    # this is simple csv output, just to show that this can be done
	    # you'll need to fix this to match the target format you need
	    # json, xml etc....
            #print('{},{}'.format(output['code'],output['title']['@value']))
            outline = f"{output['code']};{output['title']['@value']}"
            if 'definition' in output:
                outline += f";{output['definition']['@value'].replace(';','~')}"
            else:
                outline += ';'
            if 'longdefinition' in output:
                outline += f";{output['longdefinition']['@value'].replace(';','~')}"
            else:
                outline += ';'
            if 'inclusion' in output:
                outline += ';'
                for inc in output['inclusion']:
                    outline += f"§{inc['label']['@value'].replace(';','~')}"
            else:
                outline += ';'
            if 'exclusion' in output:
                outline += ';'
                for exc in output['exclusion']:
                    outline += f"§{exc['label']['@value'].replace(';','~')}"
            else:
                outline += ';'
            print(outline.encode("utf-8"))
        if 'child' in output:
            children = output['child']
            # dig deeper
            item_uris = [ base_uri.format(
                                      item.split("/mms/")[-1]
                                    ) 
                                    for item in children]
            for next_uri in item_uris:
                # trying next uri
                retrieve_code(next_uri)
    if 'parent' not in output:
        children = output['child']
        # starting at the top
        # dig deeper
        item_uris = [ base_uri.format(
                                      item.split("/mms/")[-1]
                                    ) 
                                    for item in children]
        for next_uri in item_uris:
            # next uri
            retrieve_code(next_uri)
                
print('codice;titolo;definizione;definizione lunga;inclusioni;esclusioni'.encode("utf-8"))
try:
    retrieve_code(uri)
except:
    print('-;-;-;-;-;-'.encode("utf-8"))