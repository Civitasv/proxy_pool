import requests
import math
import pandas as pd


class Hexagon:
    def __init__(self, center, radius) -> None:
        self.center = center
        self.radius = radius

    def __str__(self) -> str:
        return "center: {}, radius: {}".format(str(self.center), self.radius)


class Point(object):
    def __init__(self, lng: float, lat: float) -> None:
        self.lng = lng
        self.lat = lat

    def __str__(self) -> str:
        return "lon: {}, lat: {}".format(self.lng, self.lat)


def generate_hexagons(rect):
    hexagons = []
    r = 0.05  # 单位：度
    r_m = 6000  # 单位：m
    distance = r * math.sin(math.pi / 3) * 2  # 六边形圆心之间的距离
    x_start = rect["left"]
    y_start = rect["bottom"] + r * math.cos(math.pi / 3)

    while True:
        i = 0
        while True:
            hexagon = Hexagon(
                Point(x_start if i % 2 == 0 else x_start +
                      distance / 2, y_start), r_m
            )
            hexagons.append(hexagon)
            if y_start + r / 2 > rect["top"]:
                break
            y_start += 3 * r / 2
            i += 1
        if x_start + distance / 2 > rect["right"]:
            break
        x_start += distance

    return hexagons


# API Endpoint
BUSINESS_SEARCH = "https://api.yelp.com/v3/businesses/search"
BUSINESS_REVIEWS_SEARCH = "https://api.yelp.com/v3/businesses/{}/reviews"
"""
Parameters:
  1. (optional) term: Search term, such as 'food' and 'restaurants'
  2. location: required if either latitude or longitude is not provided.
  3. (required) latitude: latitude of the location you want to search nearby. 
  4. (required) longitude: longitude of the location you want to search nearby. 
  5. (optional) radius: a suggested search radius in meters.
  6. (optional) categories: https://www.yelp.com/developers/documentation/v3/all_category_list
  7. (optional) locale: default to en_US.
  8. (optional) limit: number to return
  9. (optional) offset: 
  10. (optional) sort_by: best_match, rating, review_count or distance.
  11. (optional) price: 
  12. (optional) open_now: default to false
  13. (optional) open_at: cannot be used with open_now
  14. (optional) attributes: 
"""

# API Key authorization
API_KEY = "IeXwtl5QBZ4FZf4Iu4hShLL95nEB3mRp2Bk8YjTrGT4CCEsmoslvKzzFqhSI7UkKgXjoqG8hl9miDPwIocDOy9EoI_ZEVub46ZjhfKbSndzQ2wZJzr5W9z84Ush4YnYx"
HEADER = {
    "Authorization": 'Bearer %s' % API_KEY
}

PER_PAGE_SIZE = 20
MAX_SIZE = 1000


def yelp_business_request(params):
    result = requests.request("GET", BUSINESS_SEARCH,
                              headers=HEADER, params=params, proxies={"https": "http://127.0.0.1:7890"})
    return result.json()


def yelp_business_reviews_request(id):
    result = requests.request(
        "GET", BUSINESS_REVIEWS_SEARCH.format(id), headers=HEADER)
    return result.json()


def yelp_search(_categories, _lng, _lat, _radius):
    params = {
        "categories": _categories,
        "longitude": _lng,
        "latitude": _lat,
        "limit": PER_PAGE_SIZE,
        "radius": _radius,
        "offset": 0,
    }

    # get total number
    print("Get total number.......")
    page_first = yelp_business_request(params)
    total = page_first["total"]
    print("Total: {} {} at {}, {} with radius {}".format(
        total, _categories, _lng, _lat, _radius))

    total = min(total, MAX_SIZE)
    offsets = [
        i * PER_PAGE_SIZE for i in range(math.ceil(total / PER_PAGE_SIZE))]

    result = []
    print("********************** Start ***********************")
    for offset in offsets:
        print("Retrieving: {}/{}".format(offset, total))
        params["offset"] = offset
        data = yelp_business_request(params)["businesses"]
        for item in data:  # extract data we need in loop
            # get basic information
            categories = item.get("categories", "")
            latitude = item["coordinates"]["latitude"]
            longitude = item["coordinates"]["longitude"]
            location = item.get("location", "")
            display_phone = item.get("display_phone", "")
            phone = item.get("phone", "")
            unique_id = item.get("id", "")
            unique_alias = item.get("alias", "")
            image_url = item.get("image_url", "")
            is_closed = item.get("is_closed", "")
            name = item.get("name", "")
            price = item.get("price", "")
            rating = item.get("rating", "")
            review_count = item.get("review_count", "")
            url = item.get("url", "")
            transactions = item.get("transactions", "")
            result.append(
                [
                    categories,
                    latitude,
                    longitude,
                    display_phone,
                    phone,
                    unique_id,
                    unique_alias,
                    image_url,
                    is_closed,
                    location,
                    name,
                    price,
                    rating,
                    review_count,
                    url,
                    transactions,
                ]
            )
    df = pd.DataFrame(
        data=result,
        columns=[
            "categories",
            "latitude",
            "longitude",
            "display_phone",
            "phone",
            "unique_id",
            "unique_alias",
            "image_url",
            "is_closed",
            "location",
            "name",
            "price",
            "rating",
            "review_count",
            "url",
            "transactions",
        ],
    )
    df.drop_duplicates("unique_id", "first", inplace=True)
    print("{} {} at {},{} with radius {} Retrieved.".format(
        len(df), _categories, _lng, _lat, _radius))

    return df


def execute():
    rect = {
        "left": -122.455368,
        "right": -122.228088,
        "top": 47.734542,
        "bottom": 47.491989,
    }

    # 1. generate hexagons
    print("generate hexagons.......")
    hexogons = generate_hexagons(rect)
    print("generate hexagons finished.......")

    # 2. start crawl
    print("Start crawl......")
    result = []
    for hexogon in hexogons:
        print("Crawl: {}".format(hexogon))
        result.append(
            yelp_search(
                "restaurants", hexogon.center.lng, hexogon.center.lat, hexogon.radius
            )
        )
        print("Finish Crawl: {}".format(hexogon))
    print("Finish crawl.......")

    data = pd.concat(result, ignore_index=True)
    print("The number of restaurants is {}".format(len(data)))
    return data


if __name__ == "__main__":
    output_file = "./restaurants.csv"
    data = execute()
    data.to_csv(output_file, index=False)
    print("the csv has been downloaded to your local computer. The program has been completed successfully.")
