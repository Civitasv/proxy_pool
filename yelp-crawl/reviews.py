# -*- coding: utf-8 -*-
"""Yelp_API Crawler_Seattle_2021

For Reviews
"""

import requests
import math
import pandas as pd
import csv
import concurrent.futures
import os
from lxml import etree


def is_contains_chinese(strs):
    for _char in strs:
        if _char >= '\u4e00' and _char <= '\u9fa5':
            return True
    return False


def read_restaurants_csv(csvfile):
    with open(csvfile, encoding="utf8") as f:
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
                    0,
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
                "recommended_review_count",
                "unrecommended_review_count",
                "url",
                "transactions",
            ],
        )
        return df


def yelp_recommended_reviews_crawler_with_api(unique_id, unique_alias):
    def yelp_recommended_reviews_request(start, retry=100):
        print("Retrieving: {}-{}".format(start, start + 10 - 1))
        params = {
            "rl": "en",
            "sort_by": "relevance_desc",
            "start": start
        }
        if retry == 0:
            return {"reviews": []}
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) " \
                    "Chrome/83.0.4103.116 Safari/537.36 "
        header = {
            "user-agent": useragent,
        }
        url = "https://www.yelp.com/biz/{}/review_feed".format(unique_id)

        try:
            result = requests.get(url, headers=header, params=params, proxies={"https": "http://127.0.0.1:24000"})
            # print(result.status_code)
            response = result.json()
            if response is None:
                # print("retry..., left {}".format(retry - 1))
                return yelp_recommended_reviews_request(start, retry - 1)
            else:
                # print("SUCCESS!")
                return result.json()
        except Exception as e:
            # print("exception retry..., left {}".format(retry - 1))
            return yelp_recommended_reviews_request(start, retry - 1)

    # get total number
    page_first = yelp_recommended_reviews_request(0)
    try:
        total = page_first["pagination"]["totalResults"]
    except Exception as e:
        print("Request error")
        total = 0
    print("unique_id: {}, unique_alias: {}, total: {}".format(
        unique_id, unique_alias, total))

    starts = [i * 10 for i in range(math.ceil(total / 10))]

    result = []
    print("********************** Start ***********************")
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        results = [executor.submit(yelp_recommended_reviews_request, start=start) for start in starts]
        for f in concurrent.futures.as_completed(results):
            item_result = f.result()
            # print(item_result)
            if item_result:
                data = item_result["reviews"]
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
    print("The number of recommended reviews for {} is {}".format(unique_alias, len(result)))

    save_to_csv(df, "recommended-reviews/" + unique_alias + "," + unique_id + ".csv")


def yelp_unrecommended_reviews_crawler_without_api(unique_id, unique_alias):
    def yelp_unrecommended_reviews_request(not_recommended_start, retry=100):
        print("Retrieving: {}-{}".format(not_recommended_start, not_recommended_start + 10 - 1))
        params = {
            "not_recommended_start": not_recommended_start
        }
        if retry == 0:
            return {"reviews": [], "total": 0}
        useragent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) " \
                    "Chrome/83.0.4103.116 Safari/537.36 "
        header = {
            "user-agent": useragent,
        }
        url = "https://www.yelp.com/not_recommended_reviews/{}".format(unique_alias)
        reviews = []
        try:
            result = requests.get(url, headers=header, params=params, proxies={"https": "http://127.0.0.1:24000"})
            # print(result.status_code)

            html = etree.HTML(result.text)
            try:
                lis = html.xpath('//*[@id="super-container"]/div[2]/div/div/div[3]/div/div[1]/ul/li')
            except Exception as e:
                print("LIS", e)
                return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
            try:
                total = html.xpath('//*[@id="super-container"]/div[2]/div/div/div[3]/div/div[1]/h3/text()')[0].split()[
                    0].strip()
            except Exception as e:
                print("TOTAL", e)
                return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
            for li in lis:
                try:
                    review_id = li.xpath('./div/@data-review-id')[0]
                except Exception as e:
                    print("REVIEWID", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    avatar = li.xpath('./div/div[1]/div/div/div[1]/div/img/@src')[0]
                except Exception as e:
                    print("AVATAR", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    username = li.xpath('./div/div[1]/div/div/div[2]/ul[1]/li[1]/span/text()')[0]
                except Exception as e:
                    print("USERNAME", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    user_location = li.xpath('./div/div[1]/div/div/div[2]/ul[1]/li[2]/b/text()')[0]
                except Exception as e:
                    user_location = ""
                    print("USERLOCATION", e)
                try:
                    temp = li.xpath('./div/div[1]/div/div/div[2]/ul[2]/li[1]/b/text()')
                    friend_count = temp[0] if len(temp) > 0 else 0
                except Exception as e:
                    print("FRIEND_COUNT", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    temp = li.xpath('./div/div[1]/div/div/div[2]/ul[2]/li[2]/b/text()')
                    review_count = temp[0] if len(temp) > 0 else 0
                except Exception as e:
                    print("REVIEW_COUNT", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    temp = li.xpath('./div/div[1]/div/div/div[2]/ul[2]/li[3]/b/text()')
                    photo_count = temp[0] if len(temp) > 0 else 0
                except Exception as e:
                    print("PHOTO_COUNT", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    user_id = li.xpath('./div/div[1]/div/div/div[2]/ul[1]/li[1]/span/@data-hovercard-id')[0]
                except Exception as e:
                    print("USER_ID", e)
                    user_id = ""
                try:
                    text = li.xpath('./div/div[2]/div[1]/p/text()')[0]
                except Exception as e:
                    print("TEXT", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    language = li.xpath('./div/div[2]/div[1]/p/@lang')[0]
                except Exception as e:
                    print("LANGUAGE", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)

                try:
                    date = li.xpath('./div/div[2]/div[1]/div/span/text()')[0].strip()
                except Exception as e:
                    print("DATE", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)
                try:
                    rating = li.xpath('./div/div[2]/div[1]/div/div/div/@title')[0].split(" ")[0]
                except Exception as e:
                    print("RATING", e)
                    return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)

                comment = {
                    "text": text,
                    "language": language
                }
                user = {
                    "src": avatar,
                    "markupDisplayName": username,
                    "displayLocation": user_location,
                    "friendCount": friend_count,
                    "reviewCount": review_count,
                    "photoCount": photo_count,
                }

                reviews.append([review_id,
                                unique_id,
                                unique_alias,
                                user_id,
                                user,
                                comment,
                                date,
                                rating,
                                ""])
            return {"reviews": reviews, "total": int(total)}
        except Exception as e:
            print("EXCEPTION", e)
            # print("exception retry..., left {}".format(retry - 1))
            return yelp_unrecommended_reviews_request(not_recommended_start, retry - 1)

    # get total number
    page_first = yelp_unrecommended_reviews_request(0)
    total = page_first["total"]
    print("unique_id: {}, unique_alias: {}, total: {}".format(
        unique_id, unique_alias, total))

    starts = [i * 10 for i in range(math.ceil(total / 10))]

    result = []
    print("********************** Start ***********************")
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        results = [executor.submit(yelp_unrecommended_reviews_request, not_recommended_start=not_recommended_start) for
                   not_recommended_start in starts]
        for f in concurrent.futures.as_completed(results):
            item_result = f.result()
            # print(item_result)
            if item_result:
                data = item_result["reviews"]
                result.extend(data)

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
    print("The number of unrecommended reviews for {} is {}".format(unique_alias, len(result)))

    save_to_csv(df, "unrecommended-reviews/" + unique_alias + "," + unique_id + ".csv")


def save_to_csv(data, filename):
    data.to_csv(filename, index=False)


def yelp_recommended_reviews():
    data = read_restaurants_csv("restaurants.csv")

    print("The number of restaurants is {}".format(len(data)))
    for index, row in data.iterrows():
        unique_id = row["unique_id"]
        unique_alias = row["unique_alias"]
        path = "recommended-reviews/" + unique_alias + "," + unique_id + ".csv"
        if os.path.exists(path):
            continue
        yelp_recommended_reviews_crawler_with_api(row["unique_id"], row["unique_alias"])


def yelp_unrecommended_reviews():
    data = read_restaurants_csv("restaurants.csv")

    print("The number of restaurants is {}".format(len(data)))
    for index, row in data.iterrows():
        unique_id = row["unique_id"]
        unique_alias = row["unique_alias"]
        path = "unrecommended-reviews/" + unique_alias + "," + unique_id + ".csv"
        if os.path.exists(path):
            continue
        yelp_unrecommended_reviews_crawler_without_api(row["unique_id"], row["unique_alias"])


if __name__ == "__main__":
    data = read_restaurants_csv("restaurants.csv")

    for index, row in data.iterrows():
        unique_id = row["unique_id"]
        unique_alias = row["unique_alias"]
        path = "unrecommended-reviews/" + unique_alias + "," + unique_id + ".csv"
        with open(path, encoding="utf8") as f:
            reader = csv.reader(f)
            lines = len(list(reader))
            data.at[index, "unrecommended_review_count"] = lines - 1

        path = "recommended-reviews/" + unique_alias + "," + unique_id + ".csv"
        with open(path, encoding="utf8") as f:
            reader = csv.reader(f)
            lines = len(list(reader))
            data.at[index, "recommended_review_count"] = lines - 1

    save_to_csv(data, "restaurants2.csv")

