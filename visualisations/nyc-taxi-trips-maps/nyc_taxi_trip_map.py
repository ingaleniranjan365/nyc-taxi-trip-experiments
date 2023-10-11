import pandas as pd
import folium


def save_map(data: pd.DataFrame, lat_col: str, lon_col: str, filename: str):
    coordinates = list(zip(data[lat_col], data[lon_col]))
    nyc_map = folium.Map(location=[40.7128, -74.0060], zoom_start=12, tiles="CartoDB dark_matter")
    radius = 1

    for lat, lon in coordinates:
        folium.Marker(
            location=[lat, lon],
            icon=folium.DivIcon(
                html=f'<div style="width: {radius}px; height: {radius}px; background-color: #ccf2ff; border-radius: 50%;"></div>')
        ).add_to(nyc_map)

    nyc_map.save(f"{filename}.html")


def create_pickup_map(data: pd.DataFrame):
    save_map(data, 'pickup_latitude', 'pickup_longitude', 'nyc_taxi_trip_pickups')


def create_dropoff_map(data: pd.DataFrame):
    save_map(data, 'dropoff_latitude', 'dropoff_longitude', 'nyc_taxi_trip_dropoffs')


def main():
    trips_path = '../../data/nyc-taxi-trip-duration-kaggle/train.csv'
    data = pd.read_csv(trips_path)
    sampled_data = data.sample(n=50000, random_state=42)
    create_pickup_map(sampled_data)
    create_dropoff_map(sampled_data)


if __name__ == '__main__':
    main()