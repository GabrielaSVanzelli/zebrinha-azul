import psycopg2
from psycopg2 import OperationalError
import logging

logger = logging.getLogger(__name__)

class Exporter:
    def __init__(self,
                 dbname:str,
                 user:str,
                 password:str,
                 host:str,
                 port:str) -> None:
        self.conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            password=password,
            host=host,
            port=port
        )
        self.cursor = self.conn.cursor()
            
    def inserir_geocoded_waypoints(self, data):
        try:
            for waypoint in data["geocoded_waypoints"]:
                self.cursor.execute("""
                    INSERT INTO waypoints (geocoder_status, place_id, types)
                    VALUES (%s, %s, %s);
                """, (
                    waypoint["geocoder_status"],
                    waypoint["place_id"],
                    waypoint["types"]
                ))
        except Exception as e:
            print(f"Erro ao inserir na tabela geocoded_waypoints: {e}")

    def inserir_routes(self, data):
        try:
            for route in data["routes"]:
                self.cursor.execute("""
                    INSERT INTO routes (bounds_ne_lat, bounds_ne_lng, bounds_sw_lat, bounds_sw_lng, copyrights, overview_polyline, summary)
                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    route["bounds"]["northeast"]["lat"],
                    route["bounds"]["northeast"]["lng"],
                    route["bounds"]["southwest"]["lat"],
                    route["bounds"]["southwest"]["lng"],
                    route["copyrights"],
                    route["overview_polyline"]["points"],
                    route["summary"]
                ))
        except Exception as e:
            print(f"Erro ao inserir na tabela routes: {e}")

    def inserir_legs(self, data):
        try:
            for route in data["routes"]:
                for leg in route["legs"]:
                    self.cursor.execute("""
                        INSERT INTO legs (route_id, distance_text, distance_value, duration_text, duration_value, end_address, end_location_lat, end_location_lng, start_address, start_location_lat, start_location_lng)
                        VALUES ((SELECT id FROM routes ORDER BY id DESC LIMIT 1), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        leg["distance"]["text"],
                        leg["distance"]["value"],
                        leg["duration"]["text"],
                        leg["duration"]["value"],
                        leg["end_address"],
                        leg["end_location"]["lat"],
                        leg["end_location"]["lng"],
                        leg["start_address"],
                        leg["start_location"]["lat"],
                        leg["start_location"]["lng"]
                    ))
        except Exception as e:
            print(f"Erro ao inserir na tabela legs: {e}")

    def inserir_steps(self, data):
        try:
            for route in data["routes"]:
                for leg in route["legs"]:
                    for step in leg["steps"]:
                        self.cursor.execute("""
                            INSERT INTO steps (leg_id, distance_text, distance_value, duration_text, duration_value, end_location_lat, end_location_lng, html_instructions, polyline, start_location_lat, start_location_lng, travel_mode, maneuver)
                            VALUES ((SELECT id FROM legs ORDER BY id DESC LIMIT 1), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                        """, (
                            step["distance"]["text"],
                            step["distance"]["value"],
                            step["duration"]["text"],
                            step["duration"]["value"],
                            step["end_location"]["lat"],
                            step["end_location"]["lng"],
                            step["html_instructions"],
                            step["polyline"]["points"],
                            step["start_location"]["lat"],
                            step["start_location"]["lng"],
                            step["travel_mode"],
                            step.get("maneuver", None)
                        ))
        except Exception as e:
            print(f"Erro ao inserir na tabela steps: {e}")

    def main(self, data):
        for body in data:
            self.inserir_geocoded_waypoints(body)
            self.inserir_routes(body)
            self.inserir_legs(body)
            self.inserir_steps(body)

        self.conn.commit()
        self.conn.close()
        self.cursor.close()


class ExporterWeather:
    def __init__(self,
                 dbname: str,
                 user: str,
                 password: str,
                 host: str,
                 port: str):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port

    def connect(self):
        """Estabelece a conexão com o banco de dados PostgreSQL."""
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                password=self.password,
                host=self.host,
                port=self.port
            )
            return conn
        except Exception as error:
            print(f"Erro ao conectar ao banco de dados: {error}")
            return None

    def create_table(self):
        """Cria a tabela weather_data no banco de dados PostgreSQL."""
        conn = self.connect()
        if conn is None:
            return
        
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS weather_data (
            id SERIAL PRIMARY KEY,
            lon FLOAT,
            lat FLOAT,
            weather_id INT,
            weather_main TEXT,
            weather_description TEXT,
            weather_icon TEXT,
            base TEXT,
            temp FLOAT,
            feels_like FLOAT,
            temp_min FLOAT,
            temp_max FLOAT,
            pressure INT,
            humidity INT,
            sea_level INT,
            grnd_level INT,
            visibility INT,
            wind_speed FLOAT,
            wind_deg INT,
            wind_gust FLOAT,
            clouds_all INT,
            dt INT,
            sys_country TEXT,
            sys_sunrise INT,
            sys_sunset INT,
            timezone INT,
            city_id INT,
            city_name TEXT,
            cod INT
        );
        """
        try:
            cursor.execute(create_table_query)
            conn.commit()
            print("Tabela criada com sucesso.")
        except Exception as error:
            print(f"Erro ao criar a tabela: {error}")
        finally:
            cursor.close()
            conn.close()

    def insert_weather_data(self, data):
        """Insere dados meteorológicos na tabela weather_data."""
        conn = self.connect()
        if conn is None:
            return

        cursor = conn.cursor()
        for body in data:
            try:
                insert_query = """
                INSERT INTO weather_data (
                    lon, lat, weather_id, weather_main, weather_description, weather_icon, base,
                    temp, feels_like, temp_min, temp_max, pressure, humidity, sea_level,
                    grnd_level, visibility, wind_speed, wind_deg, wind_gust, clouds_all, dt,
                    sys_country, sys_sunrise, sys_sunset, timezone, city_id, city_name, cod
                ) VALUES (
                    %(lon)s, %(lat)s, %(weather_id)s, %(weather_main)s, %(weather_description)s, %(weather_icon)s, %(base)s,
                    %(temp)s, %(feels_like)s, %(temp_min)s, %(temp_max)s, %(pressure)s, %(humidity)s, %(sea_level)s,
                    %(grnd_level)s, %(visibility)s, %(wind_speed)s, %(wind_deg)s, %(wind_gust)s, %(clouds_all)s, %(dt)s,
                    %(sys_country)s, %(sys_sunrise)s, %(sys_sunset)s, %(timezone)s, %(city_id)s, %(city_name)s, %(cod)s
                );
                """
                try:
                    cursor.execute(insert_query, body)
                    conn.commit()
                    print("Dados inseridos com sucesso.")
                except Exception as error:
                    print(f"Erro ao inserir os dados: {error}")
                finally:
                    cursor.close()
                    conn.close()
            except Exception as err:
                logger.error(err)
