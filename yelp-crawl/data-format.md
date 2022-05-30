# Explanation of data format

> Crape time: 05/14/2022  
> Encoding: UTF-8

## restaurants.csv

This file contains all restaurants of Seattle. The detailed explanations of each field are as follows.

|Field|Explanation|
|:----:| ---- |
|categories|Category info(include category title and alias) of this restaurant|
|latitude|Latitude of this restaurant|
|longitude|Longitude of this restaurant|
|display_phone|Phone number of this restaurant formatted nicely to be displayed to users. The format is the standard phone number format for the restaurant's country.|
|phone|Phone number of this restaurant|
|unique_id|Unique Yelp ID of this restaurant|
|unique_alias|Unique Yelp alias of this restaurant|
|image_url|URL of photo for this restaurant|
|is_closed|Whether this restaurant has been closed|
|city|City of this restaurant|
|country|Country of this restaurant|
|state|State of this restaurant|
|zip_code|ZipCode of this restaurant|
|name|Name of this restaurant|
|price|Price level of this restaurant (one of $, $$, $$$, and $$$$)|
|rating|Rating for this restaurant (value ranges from 1, 1.5, ..., 4.5, 5)|
|recommended_review_count|Number of recommended reviews of this restaurant|
|unrecommended_review_count|Number of unrecommended(fake) reviews of this restaurant|
|url|URL for business page on Yelp|
|transactions|List of Yelp transactions that the business is registered for. Current supported values are pickup, delivery and restaurant_reservation|

## (un)recommended-reviews

This directory includes all (un)recommended reviews of restaurants. The name format of each file is: `[unique_id].csv`.

And the detailed explanations of each field are as follows.

user_id,user,comment,date,rating,feedback

|Field|Explanation|
|:----:| ---- |
|review_id|Unique id of this review|
|unique_id|Unique id of corresponding restaurant of this review|
|unique_alias|Unique id of corresponding restaurant of this review|
|user_id|User id of corresponding user **(unrecommended-reviews don't have this field)**|
|user|Detailed user info|
|comment|The content and language of this review|
|date|Date of this review|
|rating|Rating of this review|
|feedback|Feedback of this review, include counts of useful, funny and cool|
