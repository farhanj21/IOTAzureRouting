import asyncio
import time
import json

from azure.iot.device.aio import IoTHubDeviceClient
from azure.iot.device import Message


async def connect():
    connection_str = "HostName=IOTAssignment2.azure-devices.net;DeviceId=VirtualDevice1;SharedAccessKey=+GuMKy8cWcPCTFFXNFKyKdpZO3yfxvBMFyEB9m+/m4I="
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_str)
    await device_client.connect()
    print('Connected to IoT Hub')
    return device_client


async def send_data(device_client, points):
    for i, point in enumerate(points):
        data = {
            "lat": point["lat"],
            "lng": point["lng"],
            "count": i
        }
        message = Message(data=json.dumps(data))
        await device_client.send_message(message)
        print(f"Sent message {i+1}: {data}")
        time.sleep(0.5)  


def read_route_file(filename):
    with open(filename, 'r') as f:
        geojson = json.load(f)

    # Extracting coordinates
    coordinates = next(
        (feature["geometry"]["coordinates"]
         for feature in geojson["features"]
         if feature["geometry"]["type"] == "LineString"),
        None
    )

    if not coordinates:
        raise ValueError("No LineString geometry found in GeoJSON.")

    return [{"lat": lat, "lng": lng} for lng, lat in coordinates]


def main():
    points = read_route_file('route1.json') 
    device_client = asyncio.run(connect())
    asyncio.run(send_data(device_client, points))
    asyncio.run(device_client.shutdown())


if __name__ == "__main__":
    main()
