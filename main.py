import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib
import json
from google.oauth2 import service_account
from posthog import Posthog
import numpy as np

# Initialize PostHog
posthog = Posthog(
    project_api_key='phc_iY1kjQZ5ib5oy0PU2fRIqJZ5323jewSS5fVDNyhe7RY',
    host='https://us.i.posthog.com'
)

# Authentication credentials
CREDENTIALS = {
    "admin": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",  # admin
    "user": "04f8996da763b7a969b1028ee3007569eaf3a635486ddab211d512c85b9df8fb",  # user
    "dina.teilab@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mai.sobhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mostafa.sayed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.hassan@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.youssef@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ahmed.nagy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "adel.abuelella@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "ammar.abdelbaset@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "youssef.mohamed@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "abdallah.hazem@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohamed.abdelgalil@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
    "mohanad.elgarhy@sylndr.com": hashlib.sha256("sylndr123".encode()).hexdigest(),
}

def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in CREDENTIALS and \
                hashlib.sha256(st.session_state["password"].encode()).hexdigest() == CREDENTIALS[
            st.session_state["username"]]:
            st.session_state["password_correct"] = True
            st.session_state["current_user"] = st.session_state["username"]  # Store the username

            # First identify the user
            posthog.identify(
                st.session_state["username"],  # Use email as distinct_id
                {
                    'email': st.session_state["username"],
                    'name': st.session_state["username"].split('@')[0].replace('.', ' ').title(),
                    'last_login': datetime.now().isoformat()
                }
            )

            # Then capture the login event
            posthog.capture(
                st.session_state["username"],
                '$login',
                {
                    'app': 'Pipeline Matcher',
                    'login_method': 'password',
                    'success': True
                }
            )

            del st.session_state["password"]  # Don't store the password
            del st.session_state["username"]  # Don't store the username
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated
    if st.session_state.get("password_correct", False):
        return True

    # Show input fields for username and password
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password")
    st.button("Login", on_click=password_entered)

    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")

    return False

# Set page config
st.set_page_config(
    page_title="Pipeline Matcher - Car to Dealer Matching",
    page_icon="🎯",
    layout="wide"
)

# Main app logic
if check_password():
    @st.cache_data(ttl=21600)  # Cache data for 6 hours
    def load_pipeline_data():
        """Load all necessary data for pipeline matching"""
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error("No credentials found. Please configure either Streamlit secrets or provide a service_account.json file.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        client = bigquery.Client(credentials=credentials)

        # Pipeline cars query
        pipeline_query = """
        SELECT opportunity_id, opportunity_creation_datetime, opportunity_code, opportunity_name, car_name, 
               car_kilometrage, car_make, car_model, car_year, car_trim, inspection_current_status, 
               sylndr_offer_price, median_asked_price, asking_price, lead_source, 
               opportunity_current_status, customer_offer_decision
        FROM reporting.acquisition_opportunity 
        WHERE is_acquired = false 
          AND opportunity_current_status <> 'Lost' 
          AND quality_decision = 'Accepted'
        ORDER BY opportunity_creation_datetime DESC 
        """

        # Live inventory query
        inventory_query = """
        with publishing AS (
        SELECT sf_vehicle_name,
               publishing_state,
               DATE(published_at) AS publishing_date,
               DATE_TRUNC(DATE(published_at),week) AS publishing_week,
               DATE_TRUNC(DATE(published_at),month) AS publishing_month,
               days_on_app AS DOA,
               MAX(published_at) over (partition by sf_vehicle_name) AS max_publish_date
        FROM ajans_dealers.ajans_wholesale_to_retail_publishing_logs
        WHERE sf_vehicle_name NOT in ("C-32211","C-32203") 
        QUALIFY published_at = max_publish_date
        ),

        selling_opportunity AS (
        SELECT a.car_name , 
               DATE(b.opportunity_creation_datetime) AS sold_date
        FROM reporting.vehicle_acquisition_to_selling a 
        LEFT JOIN reporting.wholesale_selling_opportunity b 
        ON a.selling_opportunity_id= b.opportunity_id
        WHERE a.selling_opportunity_id is NOT NULL ),

        sold_in_showroom AS (
        SELECT DISTINCT sf_vehicle_name,
               MAX(CASE WHEN request_type = "Showroom" AND request_status = "Succeeded" THEN dealer_code END) over (Partition BY sf_vehicle_name,dealer_code) AS showroom_flag,
               MAX(CASE WHEN request_type = "Buy Now" AND request_status = "Succeeded" THEN dealer_code END) over (Partition BY sf_vehicle_name,dealer_code) AS purchased_dealer
        FROM ajans_dealers.dealer_requests 
        QUALIFY showroom_flag = purchased_dealer ),

        buy_now_requests AS (
        SELECT a.sf_vehicle_name,
               MAX(consignment_date) AS consignment_date,
               MAX(wholesale_vehicle_sold_date) AS wholesale_vehicle_sold_date,
               DATE(MAX(opportunity_sold_status_datetime)) AS sf_sold_date,
               COUNT(CASE WHEN request_type = "Buy Now" THEN vehicle_request_id END) AS Buy_now_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) >= DATE(published_at) THEN vehicle_request_id END) AS Buy_now_requests_count_from_last_publishing,
               COUNT(CASE WHEN request_type = "Showroom" AND DATE(vehicle_request_created_at) >= DATE(published_at) THEN vehicle_request_id END) AS Showroom_requests_count_from_last_publishing,
               COUNT(CASE WHEN request_type = "Buy Now" AND visited_at is NOT NULL THEN vehicle_request_id END) AS Buy_now_visits_count,
               COUNT(CASE WHEN request_type = "Showroom" THEN vehicle_request_id END) AS showroom_requests_count,
               COUNT(CASE WHEN request_type = "Showroom" AND request_status = "Succeeded" THEN vehicle_request_id END) AS succ_showroom_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(published_at) = DATE(vehicle_request_created_at) THEN vehicle_request_id END) AS first_day_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(published_at) = DATE(vehicle_request_created_at) AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_from_first_day_requests,
               COUNT(CASE WHEN request_type = "Buy Now" AND ((DATE(vehicle_request_created_at) <= consignment_date) OR (consignment_date is NULL)) THEN vehicle_request_id END) AS flash_sale_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND ((DATE(vehicle_request_created_at) <= consignment_date) OR (consignment_date is NULL)) AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_in_flash_sale,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) > consignment_date THEN vehicle_request_id END) AS consignment_requests_count,
               COUNT(CASE WHEN request_type = "Buy Now" AND DATE(vehicle_request_created_at) > consignment_date AND wholesale_vehicle_sold_date is NOT NULL THEN vehicle_request_id END) AS sold_in_consignment
        FROM ajans_dealers.dealer_requests a 
        LEFT JOIN ajans_dealers.ajans_wholesale_to_retail_publishing_logs  b ON a.sf_vehicle_name = b.sf_vehicle_name
        LEFT JOIN (SELECT DISTINCT car_name,
               MAX(DATE(log_date)) AS consignment_date
        FROM ajans_dealers.wholesale_vehicle_activity_logs 
        WHERE flash_sale_enabled_before = "True" AND flash_sale_enabled_after = "False"  GROUP BY 1 ) c ON a.sf_vehicle_name = c.car_name  
        GROUP BY 1 ),

        live_cars AS (
        SELECT sf_vehicle_name,
               type AS live_status
        FROM reporting.ajans_vehicle_history 
        WHERE date_key = current_date() ),

        car_info AS (
        with max_date AS (
        SELECT sf_vehicle_name,
               event_date AS max_publish_date,
               make,
               model,
               year,
               kilometers,
               CASE WHEN (discount_enabled = True OR flash_sale_enabled = TRUE ) THEN discounted_price ELSE buy_now_price END AS App_price,
               buy_now_price,
               row_number()over(PARTITION BY sf_vehicle_name ORDER BY event_date DESC) AS row_number
        FROM ajans_dealers.vehicle_activity )

        SELECT *
        FROM max_date WHERE row_number = 1 )

        SELECT publishing.sf_vehicle_name,
               publishing_state,
               DOA,
               make,
               model,
               year,
               kilometers,
               car_condition,
               sylndr_offer_price,
               App_price,
               publishing_date,
               Buy_now_requests_count,
               Buy_now_requests_count_from_last_publishing,
               showroom_requests_count,
               succ_showroom_requests_count,
               Buy_now_visits_count,
               median_asked_price,
               current_status,
               CASE WHEN median_retail_price is NOT NULL THEN (App_price - a.median_retail_price)/a.median_retail_price
                    ELSE (App_price - a.median_asked_price)/a.median_asked_price END AS STM,
               CASE WHEN median_retail_price is NOT NULL THEN (a.sylndr_offer_price - a.median_retail_price)/a.median_retail_price
                    ELSE (a.sylndr_offer_price - a.median_asked_price)/a.median_asked_price END AS ATM
        FROM publishing
        LEFT JOIN selling_opportunity ON publishing.sf_vehicle_name = selling_opportunity.car_name
        LEFT JOIN live_cars ON publishing.sf_vehicle_name = live_cars.sf_vehicle_name
        LEFT JOIN car_info ON publishing.sf_vehicle_name = car_info.sf_vehicle_name 
        LEFT JOIN buy_now_requests ON publishing.sf_vehicle_name = buy_now_requests.sf_vehicle_name
        LEFT JOIN reporting.vehicle_acquisition_to_selling a ON publishing.sf_vehicle_name = a.car_name
        LEFT JOIN sold_in_showroom ON publishing.sf_vehicle_name = sold_in_showroom.sf_vehicle_name
        WHERE allocation_category = "Wholesale" AND current_status in ("Published" , "Being Sold")
        """

        # Dealer historical purchases
        historical_query = """
        WITH s AS (
            SELECT DISTINCT sf_vehicle_name, 
                   DATE(wholesale_vehicle_sold_date) AS request_date, 
                   dealer_code,
                   dealer_name, 
                   dealer_phone,
                   car_name,
                   CASE 
                       WHEN discount_enabled IS TRUE THEN discounted_price 
                       ELSE buy_now_price 
                   END AS price
            FROM `pricing-338819.ajans_dealers.dealer_requests`
            WHERE request_type = 'Buy Now' 
              AND wholesale_vehicle_sold_date IS NOT NULL
              AND DATE(wholesale_vehicle_sold_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
        ), p as (
            SELECT sf_vehicle_name, published_at, days_on_app
            FROM `pricing-338819.ajans_dealers.ajans_wholesale_to_retail_publishing_logs`
        ),
        cost as (
            SELECT DISTINCT car_name, sylndr_acquisition_price, market_retail_price, median_asked_price, refurbishment_cost 
            FROM `pricing-338819.reporting.daily_car_status`
        )

        SELECT s.request_date, s.dealer_code, s.dealer_name, s.dealer_phone, 
               round(p.days_on_app) as time_on_app, s.price, c.make, c.model, c.year, c.kilometers,
               cost.sylndr_acquisition_price, cost.market_retail_price
        FROM s 
        LEFT JOIN (
            SELECT DISTINCT sf_vehicle_name, make, model, year, kilometers 
            FROM `pricing-338819.reporting.ajans_vehicle_history`
        ) AS c 
        ON s.sf_vehicle_name = c.sf_vehicle_name
        LEFT JOIN cost on s.car_name = cost.car_name
        LEFT JOIN p ON s.sf_vehicle_name = p.sf_vehicle_name
        WHERE c.make IS NOT NULL
        """

        # Recent views query
        recent_views_query = """
        SELECT 
            s.time,
            s.make,
            s.model,
            s.trim,
            s.year,
            s.kilometrage,
            s.transmission,
            s.listing_title,
            s.buy_now_price,
            s.body_style,
            s.c_name,
            s.entity_code as dealer_code,
            du.dealer_user_phone as dealer_user_phone
        FROM `pricing-338819.silver_ajans_mixpanel.screen_car_profile_event` s
        LEFT JOIN ajans_dealers.dealer_users du ON s.user_code = du.dealer_user_code
        WHERE DATE(s.time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND s.entity_code IS NOT NULL
        ORDER BY s.time DESC
        """

        # Recent filters query
        recent_filters_query = """
        SELECT 
            time,
            make,
            model,
            year,
            kilometrage,
            group_filter,
            status,
            no_of_cars,
            entity_code as dealer_code
        FROM `pricing-338819.silver_ajans_mixpanel.action_filter`
        WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND entity_code IS NOT NULL
        ORDER BY time DESC
        """

        # OLX listings for all dealers
        olx_query = """
        WITH cleaned_numbers AS (
            SELECT
                DISTINCT seller_name,
                REGEXP_REPLACE(seller_phone_number, r'[^0-9,]', '') AS cleaned_phone_number,
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at
            FROM olx.listings
            WHERE added_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ),
        split_numbers AS (
            SELECT
                *,
                SPLIT(cleaned_phone_number, ',') AS phone_numbers
            FROM cleaned_numbers
        ),
        flattened_numbers AS (
            SELECT
                DISTINCT
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                seller_name,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at,
                SUBSTR(phone_number, 2) AS phone_number
            FROM split_numbers,
            UNNEST(phone_numbers) AS phone_number
        )

        SELECT 
            f.make,
            f.model,
            f.year,
            f.kilometers,
            f.price,
            f.added_at,
            d.dealer_name,
            d.dealer_code,
            d.dealer_status,
            d.dealer_email,
            d.branch_city,
            d.dealer_account_manager_name,
            d.dealer_account_manager_email
        FROM flattened_numbers f
        INNER JOIN gold_wholesale.dim_dealers d
        ON f.phone_number = d.dealer_phone
        WHERE f.make IS NOT NULL
        ORDER BY added_at DESC
        """

        print("Executing pipeline_query...")
        pipeline_df = client.query(pipeline_query).to_dataframe()
        print("✓ pipeline_query completed successfully")

        print("\nExecuting inventory_query...")
        inventory_df = client.query(inventory_query).to_dataframe()
        print("✓ inventory_query completed successfully")

        print("\nExecuting historical_query...")
        historical_df = client.query(historical_query).to_dataframe()
        print("✓ historical_query completed successfully")

        print("\nExecuting recent_views_query...")
        recent_views_df = client.query(recent_views_query).to_dataframe()
        print("✓ recent_views_query completed successfully")

        print("\nExecuting recent_filters_query...")
        recent_filters_df = client.query(recent_filters_query).to_dataframe()
        print("✓ recent_filters_query completed successfully")

        print("\nExecuting olx_query...")
        olx_df = client.query(olx_query).to_dataframe()
        print("✓ olx_query completed successfully")

        # Data preprocessing
        if not pipeline_df.empty:
            pipeline_df['opportunity_creation_datetime'] = pd.to_datetime(pipeline_df['opportunity_creation_datetime'])

        if not inventory_df.empty:
            inventory_df['publishing_date'] = pd.to_datetime(inventory_df['publishing_date'])
            # Convert numeric columns
            inventory_numeric_columns = ['year', 'kilometers', 'DOA', 'App_price', 'sylndr_offer_price',
                                         'Buy_now_requests_count', 'Buy_now_requests_count_from_last_publishing',
                                         'showroom_requests_count', 'succ_showroom_requests_count',
                                         'Buy_now_visits_count', 'median_asked_price', 'STM', 'ATM']
            for col in inventory_numeric_columns:
                if col in inventory_df.columns:
                    inventory_df[col] = pd.to_numeric(inventory_df[col], errors='coerce')

        if not historical_df.empty:
            historical_df['request_date'] = pd.to_datetime(historical_df['request_date'])
            # Convert numeric columns
            numeric_columns = ['time_on_app', 'price', 'year', 'kilometers', 'sylndr_acquisition_price', 'market_retail_price']
            for col in numeric_columns:
                if col in historical_df.columns:
                    historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce')

        if not recent_views_df.empty:
            recent_views_df['time'] = pd.to_datetime(recent_views_df['time'])

        if not recent_filters_df.empty:
            recent_filters_df['time'] = pd.to_datetime(recent_filters_df['time'])

        if not olx_df.empty:
            olx_df['added_at'] = pd.to_datetime(olx_df['added_at'])

        return pipeline_df, inventory_df, historical_df, recent_views_df, recent_filters_df, olx_df

    def calculate_dealer_match_score(car, dealer_code, historical_df, recent_views_df, recent_filters_df, olx_df):
        """Calculate how well a car matches a dealer's preferences (for pipeline cars)"""
        
        total_score = 0
        score_breakdown = {}
        
        # Get dealer data
        dealer_historical = historical_df[historical_df['dealer_code'] == dealer_code]
        dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_code]
        dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_code]
        dealer_olx = olx_df[olx_df['dealer_code'] == dealer_code]
        
        # 1. Historical Purchase Patterns (40% of total score)
        historical_score = 0
        if not dealer_historical.empty:
            # Make preference (10 points)
            make_purchases = dealer_historical[dealer_historical['make'] == car['car_make']]
            if not make_purchases.empty:
                make_frequency = len(make_purchases) / len(dealer_historical)
                historical_score += min(10, make_frequency * 30)  # Scale to max 10 points
            
            # Model preference (8 points)
            model_purchases = dealer_historical[
                (dealer_historical['make'] == car['car_make']) & 
                (dealer_historical['model'] == car['car_model'])
            ]
            if not model_purchases.empty:
                model_frequency = len(model_purchases) / len(dealer_historical)
                historical_score += min(8, model_frequency * 25)
            
            # Year preference (7 points) - within 3 years range
            if dealer_historical['year'].notna().any():
                avg_year = dealer_historical['year'].mean()
                year_diff = abs(car['car_year'] - avg_year)
                if year_diff <= 3:
                    historical_score += max(0, 7 - year_diff)
            
            # Price range preference (10 points)
            if dealer_historical['price'].notna().any():
                avg_price = dealer_historical['price'].mean()
                price_std = dealer_historical['price'].std()
                if pd.notna(car['sylndr_offer_price']) and price_std > 0:
                    price_z_score = abs((car['sylndr_offer_price'] - avg_price) / price_std)
                    if price_z_score <= 1:  # Within 1 standard deviation
                        historical_score += 10 - (price_z_score * 5)
            
            # Mileage preference (5 points)
            if dealer_historical['kilometers'].notna().any() and pd.notna(car['car_kilometrage']):
                avg_km = dealer_historical['kilometers'].mean()
                km_std = dealer_historical['kilometers'].std()
                if km_std > 0:
                    km_z_score = abs((car['car_kilometrage'] - avg_km) / km_std)
                    if km_z_score <= 1:
                        historical_score += 5 - (km_z_score * 2.5)
        
        score_breakdown['Historical Purchases'] = historical_score
        total_score += historical_score
        
        # 2. Recent App Activity (25% of total score)
        activity_score = 0
        
        # Recent views (15 points)
        if not dealer_views.empty:
            # Check for views of same make/model
            matching_views = dealer_views[
                (dealer_views['make'] == car['car_make']) | 
                (dealer_views['model'] == car['car_model'])
            ]
            if not matching_views.empty:
                view_score = min(15, len(matching_views) * 3)
                activity_score += view_score
        
        # Recent filters (10 points)
        if not dealer_filters.empty:
            matching_filters = dealer_filters[
                (dealer_filters['make'] == car['car_make']) | 
                (dealer_filters['model'] == car['car_model'])
            ]
            if not matching_filters.empty:
                filter_score = min(10, len(matching_filters) * 5)
                activity_score += filter_score
        
        score_breakdown['Recent Activity'] = activity_score
        total_score += activity_score
        
        # 3. OLX Listings (35% of total score)
        olx_score = 0
        if not dealer_olx.empty:
            # Same make listings (20 points)
            same_make_olx = dealer_olx[dealer_olx['make'] == car['car_make']]
            if not same_make_olx.empty:
                olx_score += min(20, len(same_make_olx) * 2)
            
            # Same model listings (15 points)
            same_model_olx = dealer_olx[
                (dealer_olx['make'] == car['car_make']) & 
                (dealer_olx['model'] == car['car_model'])
            ]
            if not same_model_olx.empty:
                olx_score += min(15, len(same_model_olx) * 5)
        
        score_breakdown['OLX Listings'] = olx_score
        total_score += olx_score
        
        return total_score, score_breakdown

    def get_top_dealers_for_car(car, historical_df, recent_views_df, recent_filters_df, olx_df, top_n=10):
        """Get top matching dealers for a specific car (pipeline cars)"""
        
        # Get all unique dealers
        all_dealers = set()
        if not historical_df.empty:
            all_dealers.update(historical_df['dealer_code'].unique())
        if not recent_views_df.empty:
            all_dealers.update(recent_views_df['dealer_code'].unique())
        if not recent_filters_df.empty:
            all_dealers.update(recent_filters_df['dealer_code'].unique())
        if not olx_df.empty:
            all_dealers.update(olx_df['dealer_code'].unique())
        
        dealer_scores = []
        
        for dealer_code in all_dealers:
            if pd.isna(dealer_code):
                continue
                
            score, breakdown = calculate_dealer_match_score(
                car, dealer_code, historical_df, recent_views_df, recent_filters_df, olx_df
            )
            
            # Get dealer name
            dealer_name = "Unknown"
            if not historical_df.empty:
                dealer_info = historical_df[historical_df['dealer_code'] == dealer_code]
                if not dealer_info.empty:
                    dealer_name = dealer_info['dealer_name'].iloc[0]
            
            if score > 0:  # Only include dealers with some relevance
                dealer_scores.append({
                    'dealer_code': dealer_code,
                    'dealer_name': dealer_name,
                    'match_score': score,
                    'score_breakdown': breakdown
                })
        
        # Sort by score and return top N
        dealer_scores = sorted(dealer_scores, key=lambda x: x['match_score'], reverse=True)
        return dealer_scores[:top_n]

    def calculate_inventory_match_score(car, dealer_code, historical_df, recent_views_df, recent_filters_df, olx_df):
        """Calculate how well an inventory car matches a dealer's preferences"""
        
        total_score = 0
        score_breakdown = {}
        
        # Get dealer data
        dealer_historical = historical_df[historical_df['dealer_code'] == dealer_code]
        dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_code]
        dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_code]
        dealer_olx = olx_df[olx_df['dealer_code'] == dealer_code]
        
        # 1. Historical Purchase Patterns (40% of total score)
        historical_score = 0
        if not dealer_historical.empty:
            # Make preference (10 points)
            make_purchases = dealer_historical[dealer_historical['make'] == car['make']]
            if not make_purchases.empty:
                make_frequency = len(make_purchases) / len(dealer_historical)
                historical_score += min(10, make_frequency * 30)  # Scale to max 10 points
            
            # Model preference (8 points)
            model_purchases = dealer_historical[
                (dealer_historical['make'] == car['make']) & 
                (dealer_historical['model'] == car['model'])
            ]
            if not model_purchases.empty:
                model_frequency = len(model_purchases) / len(dealer_historical)
                historical_score += min(8, model_frequency * 25)
            
            # Year preference (7 points) - within 3 years range
            if dealer_historical['year'].notna().any():
                avg_year = dealer_historical['year'].mean()
                year_diff = abs(car['year'] - avg_year)
                if year_diff <= 3:
                    historical_score += max(0, 7 - year_diff)
            
            # Price range preference (10 points)
            if dealer_historical['price'].notna().any():
                avg_price = dealer_historical['price'].mean()
                price_std = dealer_historical['price'].std()
                if pd.notna(car['App_price']) and price_std > 0:
                    price_z_score = abs((car['App_price'] - avg_price) / price_std)
                    if price_z_score <= 1:  # Within 1 standard deviation
                        historical_score += 10 - (price_z_score * 5)
            
            # Mileage preference (5 points)
            if dealer_historical['kilometers'].notna().any() and pd.notna(car['kilometers']):
                avg_km = dealer_historical['kilometers'].mean()
                km_std = dealer_historical['kilometers'].std()
                if km_std > 0:
                    km_z_score = abs((car['kilometers'] - avg_km) / km_std)
                    if km_z_score <= 1:
                        historical_score += 5 - (km_z_score * 2.5)
        
        score_breakdown['Historical Purchases'] = historical_score
        total_score += historical_score
        
        # 2. Recent App Activity (25% of total score)
        activity_score = 0
        
        # Recent views (15 points)
        if not dealer_views.empty:
            # Check for views of same make/model
            matching_views = dealer_views[
                (dealer_views['make'] == car['make']) | 
                (dealer_views['model'] == car['model'])
            ]
            if not matching_views.empty:
                view_score = min(15, len(matching_views) * 3)
                activity_score += view_score
        
        # Recent filters (10 points)
        if not dealer_filters.empty:
            matching_filters = dealer_filters[
                (dealer_filters['make'] == car['make']) | 
                (dealer_filters['model'] == car['model'])
            ]
            if not matching_filters.empty:
                filter_score = min(10, len(matching_filters) * 5)
                activity_score += filter_score
        
        score_breakdown['Recent Activity'] = activity_score
        total_score += activity_score
        
        # 3. OLX Listings (35% of total score)
        olx_score = 0
        if not dealer_olx.empty:
            # Same make listings (20 points)
            same_make_olx = dealer_olx[dealer_olx['make'] == car['make']]
            if not same_make_olx.empty:
                olx_score += min(20, len(same_make_olx) * 2)
            
            # Same model listings (15 points)
            same_model_olx = dealer_olx[
                (dealer_olx['make'] == car['make']) & 
                (dealer_olx['model'] == car['model'])
            ]
            if not same_model_olx.empty:
                olx_score += min(15, len(same_model_olx) * 5)
        
        score_breakdown['OLX Listings'] = olx_score
        total_score += olx_score
        
        return total_score, score_breakdown

    def get_top_dealers_for_inventory_car(car, historical_df, recent_views_df, recent_filters_df, olx_df, top_n=10):
        """Get top matching dealers for a specific inventory car"""
        
        # Get all unique dealers
        all_dealers = set()
        if not historical_df.empty:
            all_dealers.update(historical_df['dealer_code'].unique())
        if not recent_views_df.empty:
            all_dealers.update(recent_views_df['dealer_code'].unique())
        if not recent_filters_df.empty:
            all_dealers.update(recent_filters_df['dealer_code'].unique())
        if not olx_df.empty:
            all_dealers.update(olx_df['dealer_code'].unique())
        
        dealer_scores = []
        
        for dealer_code in all_dealers:
            if pd.isna(dealer_code):
                continue
                
            score, breakdown = calculate_inventory_match_score(
                car, dealer_code, historical_df, recent_views_df, recent_filters_df, olx_df
            )
            
            # Get dealer name
            dealer_name = "Unknown"
            if not historical_df.empty:
                dealer_info = historical_df[historical_df['dealer_code'] == dealer_code]
                if not dealer_info.empty:
                    dealer_name = dealer_info['dealer_name'].iloc[0]
            
            if score > 0:  # Only include dealers with some relevance
                dealer_scores.append({
                    'dealer_code': dealer_code,
                    'dealer_name': dealer_name,
                    'match_score': score,
                    'score_breakdown': breakdown
                })
        
        # Sort by score and return top N
        dealer_scores = sorted(dealer_scores, key=lambda x: x['match_score'], reverse=True)
        return dealer_scores[:top_n]

    def main():
        st.title("🎯 Pipeline Matcher - Car to Dealer Matching")
        
        # Track page view
        if "current_user" in st.session_state:
            posthog.capture(
                st.session_state["current_user"],
                'page_view',
                {
                    'page': 'pipeline_matcher',
                    'timestamp': datetime.now().isoformat()
                }
            )

        # Load data
        with st.spinner("Loading pipeline and dealer data..."):
            pipeline_df, inventory_df, historical_df, recent_views_df, recent_filters_df, olx_df = load_pipeline_data()

        if pipeline_df.empty:
            st.warning("No pipeline data available.")
            return

        # Display summary metrics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Pipeline Cars", len(pipeline_df))
        with col2:
            avg_price = pipeline_df['sylndr_offer_price'].mean()
            st.metric("Avg Pipeline Price", f"EGP {avg_price:,.0f}")
        with col3:
            # Fix datetime comparison by using timezone-aware datetime
            cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=7)
            recent_opportunities = len(pipeline_df[
                pipeline_df['opportunity_creation_datetime'] >= cutoff_date
            ])
            st.metric("New This Week", recent_opportunities)

        # Main tabs
        tab1, tab2 = st.tabs(["🎯 Car Matching", "📦 Inventory Matching"])

        with tab1:
            st.subheader("Car to Dealer Matching")
            
            # Car selection
            car_options = []
            for _, car in pipeline_df.iterrows():
                car_desc = f"{car['car_make']} {car['car_model']} {car['car_year']} - {car['opportunity_name']}"
                car_options.append((car_desc, car))
            
            selected_car_desc = st.selectbox(
                "Select a car from the pipeline:",
                [desc for desc, _ in car_options]
            )
            
            if selected_car_desc:
                # Get the selected car
                selected_car = next(car for desc, car in car_options if desc == selected_car_desc)
                
                # Display car details
                col1, col2 = st.columns([2, 1])
                
                with col1:
                    st.write("**Car Details:**")
                    st.write(f"• Make: {selected_car['car_make']}")
                    st.write(f"• Model: {selected_car['car_model']}")
                    st.write(f"• Year: {selected_car['car_year']}")
                    st.write(f"• Mileage: {selected_car['car_kilometrage']:,.0f} km")
                    st.write(f"• Asking Price: EGP {selected_car['asking_price']:,.0f}")
                    st.write(f"• Sylndr Offer: EGP {selected_car['sylndr_offer_price']:,.0f}")
                    st.write(f"• Status: {selected_car['opportunity_current_status']}")
                
                with col2:
                    st.write("**Opportunity Info:**")
                    st.write(f"• ID: {selected_car['opportunity_code']}")
                    st.write(f"• Lead Source: {selected_car['lead_source']}")
                    st.write(f"• Created: {selected_car['opportunity_creation_datetime'].strftime('%Y-%m-%d')}")
                    st.write(f"• Inspection: {selected_car['inspection_current_status']}")

                # Get matching dealers using the old function with renamed parameters
                with st.spinner("Finding matching dealers..."):
                    # Create a car dict compatible with the original function
                    car_for_matching = {
                        'car_make': selected_car['car_make'],
                        'car_model': selected_car['car_model'],
                        'car_year': selected_car['car_year'],
                        'car_kilometrage': selected_car['car_kilometrage'],
                        'sylndr_offer_price': selected_car['sylndr_offer_price']
                    }
                    
                    top_dealers = get_top_dealers_for_car(
                        car_for_matching, historical_df, recent_views_df, recent_filters_df, olx_df, top_n=15
                    )

                if top_dealers:
                    st.subheader("🎯 Top Matching Dealers")
                    
                    # Create dataframe for display
                    dealers_data = []
                    for dealer in top_dealers:
                        dealers_data.append({
                            'Dealer Code': dealer['dealer_code'],
                            'Dealer Name': dealer['dealer_name'],
                            'Match Score': f"{dealer['match_score']:.1f}",
                            'Historical': f"{dealer['score_breakdown'].get('Historical Purchases', 0):.1f}",
                            'Activity': f"{dealer['score_breakdown'].get('Recent Activity', 0):.1f}",
                            'OLX': f"{dealer['score_breakdown'].get('OLX Listings', 0):.1f}"
                        })
                    
                    dealers_df = pd.DataFrame(dealers_data)
                    
                    # Display with color coding
                    def highlight_score(val):
                        try:
                            score = float(val)
                            if score >= 50:
                                return 'background-color: #90EE90'  # Light green
                            elif score >= 30:
                                return 'background-color: #FFE4B5'  # Light orange
                            elif score >= 15:
                                return 'background-color: #FFCCCB'  # Light red
                            else:
                                return ''
                        except:
                            return ''
                    
                    st.dataframe(
                        dealers_df.style.applymap(highlight_score, subset=['Match Score']),
                        use_container_width=True
                    )
                    
                    st.info("🟢 High Match (50+) | 🟡 Medium Match (30-49) | 🔴 Low Match (15-29)")
                    
                    # Detailed analysis for top 3 dealers
                    st.subheader("📋 Detailed Analysis - Top 3 Dealers")
                    
                    for i, dealer in enumerate(top_dealers[:3]):
                        with st.expander(f"{i+1}. {dealer['dealer_name']} (Score: {dealer['match_score']:.1f})"):
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write("**Score Breakdown:**")
                                for component, score in dealer['score_breakdown'].items():
                                    st.write(f"• {component}: {score:.1f} points")
                            
                            with col2:
                                # Show relevant dealer activity
                                dealer_hist = historical_df[historical_df['dealer_code'] == dealer['dealer_code']]
                                dealer_olx_cars = olx_df[olx_df['dealer_code'] == dealer['dealer_code']]
                                
                                if not dealer_hist.empty:
                                    st.write("**Recent Purchases:**")
                                    recent_purchases = dealer_hist.head(3)
                                    for _, purchase in recent_purchases.iterrows():
                                        st.write(f"• {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")
                                
                                if not dealer_olx_cars.empty:
                                    st.write("**OLX Listings:**")
                                    recent_olx = dealer_olx_cars.head(3)
                                    for _, listing in recent_olx.iterrows():
                                        st.write(f"• {listing['make']} {listing['model']} ({listing['year']}) - EGP {listing['price']:,.0f}")

                else:
                    st.warning("No matching dealers found for this car.")

        with tab2:
            st.subheader("📦 Inventory to Dealer Matching")
            
            if inventory_df.empty:
                st.warning("No inventory data available.")
            else:
                # Display summary
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Total Cars", len(inventory_df))
                with col2:
                    avg_doa = inventory_df['DOA'].mean()
                    st.metric("Avg DOA", f"{avg_doa:.1f} days")
                with col3:
                    avg_requests = inventory_df['Buy_now_requests_count'].mean()
                    st.metric("Avg Requests", f"{avg_requests:.1f}")
                with col4:
                    avg_price = inventory_df['App_price'].mean()
                    st.metric("Avg Price", f"EGP {avg_price:,.0f}")
                
                # Car selection for matching
                st.write("**Select a car to find interested dealers:**")
                car_options = []
                for _, car in inventory_df.head(100).iterrows():  # Show top 100 cars
                    car_desc = f"{car['sf_vehicle_name']} - {car['make']} {car['model']} {car['year']} ({car['DOA']} days, {car['Buy_now_requests_count']} requests)"
                    car_options.append((car_desc, car))
                
                if car_options:
                    selected_car_desc = st.selectbox(
                        "Select a car from inventory:",
                        [desc for desc, _ in car_options],
                        key="inv_car_select"
                    )
                    
                    if selected_car_desc:
                        selected_car = next(car for desc, car in car_options if desc == selected_car_desc)
                        
                        # Display car details
                        col1, col2 = st.columns([2, 1])
                        
                        with col1:
                            st.write("**Car Details:**")
                            st.write(f"• Vehicle: {selected_car['sf_vehicle_name']}")
                            st.write(f"• Make: {selected_car['make']}")
                            st.write(f"• Model: {selected_car['model']}")
                            st.write(f"• Year: {selected_car['year']}")
                            st.write(f"• Mileage: {selected_car['kilometers']:,.0f} km")
                            st.write(f"• App Price: EGP {selected_car['App_price']:,.0f}")
                            st.write(f"• Days on App: {selected_car['DOA']} days")
                        
                        with col2:
                            st.write("**Performance Metrics:**")
                            st.write(f"• Buy Now Requests: {selected_car['Buy_now_requests_count']}")
                            st.write(f"• Showroom Requests: {selected_car['showroom_requests_count']}")
                            st.write(f"• Visits: {selected_car['Buy_now_visits_count']}")
                            st.write(f"• Publishing State: {selected_car['publishing_state']}")
                            if pd.notna(selected_car['STM']):
                                st.write(f"• STM: {selected_car['STM']:.2%}")

                        # Get matching dealers for this inventory car
                        with st.spinner("Finding interested dealers..."):
                            top_dealers = get_top_dealers_for_inventory_car(
                                selected_car, historical_df, recent_views_df, recent_filters_df, olx_df, top_n=15
                            )

                        if top_dealers:
                            st.subheader("🎯 Most Interested Dealers")
                            
                            # Create dataframe for display
                            dealers_data = []
                            for dealer in top_dealers:
                                dealers_data.append({
                                    'Dealer Code': dealer['dealer_code'],
                                    'Dealer Name': dealer['dealer_name'],
                                    'Interest Score': f"{dealer['match_score']:.1f}",
                                    'Historical': f"{dealer['score_breakdown'].get('Historical Purchases', 0):.1f}",
                                    'Activity': f"{dealer['score_breakdown'].get('Recent Activity', 0):.1f}",
                                    'OLX': f"{dealer['score_breakdown'].get('OLX Listings', 0):.1f}"
                                })
                            
                            dealers_df = pd.DataFrame(dealers_data)
                            
                            # Display with color coding
                            def highlight_score(val):
                                try:
                                    score = float(val)
                                    if score >= 50:
                                        return 'background-color: #90EE90'  # Light green
                                    elif score >= 30:
                                        return 'background-color: #FFE4B5'  # Light orange
                                    elif score >= 15:
                                        return 'background-color: #FFCCCB'  # Light red
                                    else:
                                        return ''
                                except:
                                    return ''
                            
                            st.dataframe(
                                dealers_df.style.applymap(highlight_score, subset=['Interest Score']),
                                use_container_width=True
                            )
                            
                            st.info("🟢 High Interest (50+) | 🟡 Medium Interest (30-49) | 🔴 Low Interest (15-29)")
                            
                            # Show top 3 dealers analysis
                            st.subheader("📋 Top 3 Most Interested Dealers")
                            
                            for i, dealer in enumerate(top_dealers[:3]):
                                with st.expander(f"{i+1}. {dealer['dealer_name']} (Score: {dealer['match_score']:.1f})"):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.write("**Interest Breakdown:**")
                                        for component, score in dealer['score_breakdown'].items():
                                            st.write(f"• {component}: {score:.1f} points")
                                    
                                    with col2:
                                        # Show relevant dealer activity
                                        dealer_hist = historical_df[historical_df['dealer_code'] == dealer['dealer_code']]
                                        dealer_olx_cars = olx_df[olx_df['dealer_code'] == dealer['dealer_code']]
                                        
                                        if not dealer_hist.empty:
                                            st.write("**Recent Purchases:**")
                                            similar_purchases = dealer_hist[
                                                (dealer_hist['make'] == selected_car['make']) |
                                                (dealer_hist['model'] == selected_car['model'])
                                            ].head(3)
                                            
                                            if not similar_purchases.empty:
                                                for _, purchase in similar_purchases.iterrows():
                                                    st.write(f"✅ {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")
                                            else:
                                                # Show any recent purchases
                                                recent_purchases = dealer_hist.head(2)
                                                for _, purchase in recent_purchases.iterrows():
                                                    st.write(f"• {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")
                                        
                                        if not dealer_olx_cars.empty:
                                            st.write("**OLX Listings:**")
                                            similar_olx = dealer_olx_cars[
                                                (dealer_olx_cars['make'] == selected_car['make']) |
                                                (dealer_olx_cars['model'] == selected_car['model'])
                                            ].head(3)
                                            
                                            if not similar_olx.empty:
                                                for _, listing in similar_olx.iterrows():
                                                    st.write(f"✅ {listing['make']} {listing['model']} ({listing['year']}) - EGP {listing['price']:,.0f}")

                        else:
                            st.warning("No interested dealers found for this car.")
                else:
                    st.info("No cars available in inventory.")

    if __name__ == "__main__":
        main() 
