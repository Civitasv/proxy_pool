# -*- coding: utf-8 -*-
"""Yelp_API Crawler_Seattle_2021

For Reviews
"""

import requests
import math
import pandas as pd
import csv


def read_restaurants_csv(csvfile):
    with open(csvfile) as f:
        result = []
        reader = csv.reader(f)
        i = 0
        for item in reader:  # extract data we need in loop
            if i == 0:
                i = 1
                continue
            # get basic information
            categories = item[0]
            latitude = item[1]
            longitude = item[2]
            display_phone = item[3]
            phone = item[4]
            unique_id = item[5]
            unique_alias = item[6]
            image_url = item[7]
            is_closed = item[8]
            location = item[9]
            name = item[10]
            price = item[11]
            rating = item[12]
            review_count = item[13]
            url = item[14]
            transactions = item[15]
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
        return df


def yelp_recommended_reviews_crawler_with_api(unique_id, unique_alias):
    def yelp_recommended_reviews_request(params):
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) " \
                    "Chrome/83.0.4103.116 Safari/537.36 "
        header = {
            "user-agent": useragent,
        }
        url = "https://www.yelp.com/biz/{}/review_feed".format(unique_id)

        result = requests.request("GET", url, headers=header, params=params)
        return result.json()

    params = {
        "rl": "en",
        "sort_by": "relevance_desc",
        "start": (1 - 1) * 10
    }

    # get total number
    print("Get total number.......")
    page_first = yelp_recommended_reviews_request(params)
    total = page_first["pagination"]["totalResults"]
    print("unique_id: {}, unique_alias: {}, total: {}".format(
        unique_id, unique_alias, total))

    starts = [i * 10 for i in range(math.ceil(total / 10))]

    result = []
    print("********************** Start ***********************")
    for start in starts:
        print("Retrieving: {}-{}, Total: {}".format(start, start + 10 - 1, total))
        params["start"] = start
        data = yelp_recommended_reviews_request(params)["reviews"]
        for item in data:  # extract data we need in loop
            # get basic information
            review_id = item.get("id", "")
            unique_id = unique_id
            unique_alias = unique_alias
            user_id = item.get("userId", "")
            user = item.get("user", "")
            comment = item.get("comment", "")
            date = item.get("localizedDate", "")
            rating = item.get("rating", "")
            feedback = item.get("feedback", "")
            result.append(
                [
                    review_id,
                    unique_id,
                    unique_alias,
                    user_id,
                    user,
                    comment,
                    date,
                    rating,
                    feedback
                ]
            )

    df = pd.DataFrame(
        data=result,
        columns=[
            "review_id",
            "unique_id",
            "unique_alias",
            "user_id",
            "user",
            "comment",
            "date",
            "rating",
            "feedback"
        ],
    )
    df.drop_duplicates("review_id", "first", inplace=True)
    print("The number of reviews for {} is {}".format(unique_alias, len(result)))

    return df


if __name__ == "__main__":
    result = []
    data = read_restaurants_csv("restaurants.csv")
    print("The number of restaurants is {}".format(len(data)))
    # for index, row in data.iterrows():
    #     unique_id = row["unique_id"]
    #     unique_alias = row["unique_alias"]
    #     item = yelp_recommended_reviews_crawler_with_api(
    #         unique_id, unique_alias)
    #     result.append(item)

    # reviews = pd.concat(result, ignore_index=True)
