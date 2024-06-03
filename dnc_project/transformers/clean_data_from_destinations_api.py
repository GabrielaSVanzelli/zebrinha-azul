import polyline

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

# def decode_polylines(data):
#     for entry in data:
#         if isinstance(entry, dict):
#             for route in entry.get('routes', []):
#                 for leg in route.get('legs', []):
#                     for step in leg.get('steps', []):
#                         if 'polyline' in step:
#                             step['polyline'] = polyline.decode(step['polyline']['points'])
#     return data

@transformer
def transform(data, *args, **kwargs):
    """
    """
    cleaned_data = []
    # dat_poly = decode_polylines(data)
    for entry in data:
        print(entry)  # Diagnóstico: Verificar conteúdo de entry
        if isinstance(entry, dict):  # Verificar se entry é um dicionário
            # Remover duplicatas em geocoded_waypoints e routes
            # if 'geocoded_waypoints' in entry and isinstance(entry['geocoded_waypoints'], list):
            #     entry['geocoded_waypoints'] = list({v['place_id']: v for v in entry['geocoded_waypoints']}.values())
            # if 'routes' in entry and isinstance(entry['routes'], list):
            #     entry['routes'] = list({v['summary']: v for v in entry['routes']}.values())

            # Padronizar formatos, tratar valores nulos ou ausentes
            for route in entry.get('routes', []):
                for leg in route.get('legs', []):
                    # Padronizar formatos de distância e duração
                    leg['distance']['value'] = int(leg['distance']['value']) if 'value' in leg['distance'] and leg['distance']['value'] else 0
                    leg['duration']['value'] = int(leg['duration']['value']) if 'value' in leg['duration'] and leg['duration']['value'] else 0
                    
                    # Tratamento de valores nulos
                    leg['end_address'] = leg['end_address'] or 'Unknown'
                    leg['start_address'] = leg['start_address'] or 'Unknown'
                    leg['end_location']['lat'] = float(leg['end_location']['lat']) if 'lat' in leg['end_location'] and leg['end_location']['lat'] else 0.0
                    leg['end_location']['lng'] = float(leg['end_location']['lng']) if 'lng' in leg['end_location'] and leg['end_location']['lng'] else 0.0
                    leg['start_location']['lat'] = float(leg['start_location']['lat']) if 'lat' in leg['start_location'] and leg['start_location']['lat'] else 0.0
                    leg['start_location']['lng'] = float(leg['start_location']['lng']) if 'lng' in leg['start_location'] and leg['start_location']['lng'] else 0.0
        
        cleaned_data.append(entry)
    return cleaned_data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output) != 50, 'The output is incompleted'
