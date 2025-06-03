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
    # Define car groups by origin
    CAR_GROUPS = {
        'Japanese': ['Toyota', 'Honda', 'Nissan', 'Mazda', 'Subaru', 'Mitsubishi', 'Lexus', 'Infiniti', 'Acura'],
        'German': ['BMW', 'Mercedes-Benz', 'Mercedes', 'Audi', 'Volkswagen', 'Porsche', 'Mini', 'Opel'],
        'Chinese': ['Chery', 'Geely', 'BYD', 'MG', 'Changan', 'JAC', 'Dongfeng', 'Brilliance'],
        'Korean': ['Hyundai', 'Kia', 'Genesis', 'SsangYong', 'Daewoo'],
        'American': ['Ford', 'Chevrolet', 'Cadillac', 'Jeep', 'Chrysler', 'Dodge', 'Lincoln', 'GMC'],
        'French': ['Peugeot', 'Renault', 'Citroën', 'DS'],
        'Italian': ['Fiat', 'Alfa Romeo', 'Lancia', 'Ferrari', 'Lamborghini', 'Maserati'],
        'British': ['Land Rover', 'Range Rover', 'Jaguar', 'Bentley', 'Rolls-Royce', 'Aston Martin'],
        'Swedish': ['Volvo', 'Saab'],
        'Czech': ['Skoda']
    }


    def get_car_group(make):
        """Get the origin group for a car make"""
        for group, makes in CAR_GROUPS.items():
            if make in makes:
                return group
        return 'Other'


    def get_mileage_segment(km):
        """Get mileage segment for a car"""
        if pd.isna(km):
            return None
        if km <= 30000:
            return "0-30K"
        elif km <= 60000:
            return "30K-60K"
        elif km <= 90000:
            return "60K-90K"
        elif km <= 120000:
            return "90K-120K"
        else:
            return "120K+"


    def get_price_segment(price):
        """Get price segment for a car"""
        if pd.isna(price):
            return None
        if price <= 600000:
            return "0-600K"
        elif price <= 800000:
            return "600K-800K"
        elif price <= 900000:
            return "800K-900K"
        elif price <= 1100000:
            return "900K-1.1M"
        elif price <= 1300000:
            return "1.1M-1.3M"
        elif price <= 1600000:
            return "1.3M-1.6M"
        elif price <= 2100000:
            return "1.6M-2.1M"
        else:
            return "2.1M+"


    def get_year_segment(year):
        """Get year segment for a car"""
        if pd.isna(year):
            return None
        if 2010 <= year <= 2016:
            return "2010-2016"
        elif 2017 <= year <= 2019:
            return "2017-2019"
        elif 2020 <= year <= 2021:
            return "2020-2021"
        elif 2022 <= year <= 2024:
            return "2022-2024"
        else:
            return None  # Outside defined ranges


    def show_methodology():
        """Display methodology explanation in an expander"""
        with st.expander("🔍 **How the Matching Algorithm Works**", expanded=False):
            st.markdown("""
            ### 📊 **Scoring Methodology (120 Points Total)**

            #### **🎯 Historical Purchase Patterns (40 points)**
            - **Exact Model Match** (15 pts max): If dealer bought this exact model before
            - **Same Make Match** (10 pts max): If dealer bought this brand (when no exact model)
            - **Same Origin Group** (6 pts max): If dealer bought cars from same region (Japanese/German/etc.)
            - **Price Range Segment** (4 pts max): If dealer bought cars in same price segment (0-600K, 600K-800K, 800K-900K, 900K-1.1M, 1.1M-1.3M, 1.3M-1.6M, 1.6M-2.1M, 2.1M+)
            - **Year Segment** (3 pts max): If dealer bought cars in same year segment (2010-2016, 2017-2019, 2020-2021, 2022-2024)
            - **Mileage Segment** (2 pts max): If dealer bought cars in same mileage segment (0-30K, 30K-60K, 60K-90K, 90K-120K, 120K+)

            #### **📱 App Activity (40 points)**
            - **Exact Model Requests** (12 pts max): Recent buy now/showroom requests for this model
            - **Same Make Requests** (8 pts max): Recent requests for this brand
            - **Exact Model Views** (8 pts max): Recent views of this exact model
            - **Same Make Views** (5 pts max): Recent views of this brand
            - **Exact Model Filters** (4 pts max): Recent searches for this model
            - **Same Make Filters** (3 pts max): Recent searches for this brand

            #### **🏪 OLX Market Activity (40 points)**
            - **Exact Model Listings** (15 pts max): Dealer actively sells this exact model
            - **Same Make Listings** (10 pts max): Dealer sells this brand (when no exact model)
            - **Same Origin Group** (6 pts max): Dealer sells cars from same region (Japanese/German/etc.)
            - **Price Range Segment** (4 pts max): If dealer lists cars in same price segment (0-600K, 600K-800K, 800K-900K, 900K-1.1M, 1.1M-1.3M, 1.3M-1.6M, 1.6M-2.1M, 2.1M+)
            - **Year Segment** (3 pts max): If dealer lists cars in same year segment (2010-2016, 2017-2019, 2020-2021, 2022-2024)
            - **Mileage Segment** (2 pts max): If dealer lists cars in same mileage segment (0-30K, 30K-60K, 60K-90K, 90K-120K, 120K+)

            ---

            #### **🎯 **Scoring Priority Logic:**
            1. **Model-specific behavior** gets highest scores
            2. **Make-specific behavior** gets medium scores  
            3. **Origin group behavior** gets lower scores
            4. **Price similarity** provides additional context with tight ±2% tolerance

            #### **📅 **Data Timeframes:**
            - **Historical Purchases**: Last 12 months
            - **App Activity**: Last 60 days  
            - **OLX Listings**: Last 90 days
            - **Dealer Requests**: Last 60 days (not purchased)

            #### **🎨 **Result Categories:**
            - 🟢 **High Interest (60+)**: Strong likelihood of purchase
            - 🟡 **Medium Interest (40-59)**: Moderate potential
            - 🔴 **Low Interest (20-39)**: Some relevance but lower priority
            """)


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
                st.error(
                    "No credentials found. Please configure either Streamlit secrets or provide a service_account.json file.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

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
          and customer_response_had_accepted_status = true
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

        # Dealer requests query (unpurchased requests)
        dealer_requests_query = """
        SELECT 
            vehicle_request_created_at,
            dealer_code,
            dealer_name,
            dealer_phone,
            request_type,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            request_status,
            visited_at,
            sf_vehicle_name
        FROM `pricing-338819.ajans_dealers.dealer_requests`
        WHERE wholesale_vehicle_sold_date IS NULL
        AND DATE(vehicle_request_created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
        AND dealer_code IS NOT NULL
        ORDER BY vehicle_request_created_at DESC
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

        print("\nExecuting dealer_requests_query...")
        dealer_requests_df = client.query(dealer_requests_query).to_dataframe()
        print("✓ dealer_requests_query completed successfully")

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
            numeric_columns = ['time_on_app', 'price', 'year', 'kilometers', 'sylndr_acquisition_price',
                               'market_retail_price']
            for col in numeric_columns:
                if col in historical_df.columns:
                    historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce')

        if not recent_views_df.empty:
            recent_views_df['time'] = pd.to_datetime(recent_views_df['time'])

        if not recent_filters_df.empty:
            recent_filters_df['time'] = pd.to_datetime(recent_filters_df['time'])

        if not dealer_requests_df.empty:
            dealer_requests_df['vehicle_request_created_at'] = pd.to_datetime(
                dealer_requests_df['vehicle_request_created_at'])
            dealer_requests_df['visited_at'] = pd.to_datetime(dealer_requests_df['visited_at'])
            # Convert numeric columns in dealer requests data
            requests_numeric_columns = ['car_year', 'car_kilometrage', 'buy_now_price']
            for col in requests_numeric_columns:
                if col in dealer_requests_df.columns:
                    dealer_requests_df[col] = pd.to_numeric(dealer_requests_df[col], errors='coerce')

        if not olx_df.empty:
            olx_df['added_at'] = pd.to_datetime(olx_df['added_at'])
            # Convert numeric columns in OLX data
            olx_numeric_columns = ['year', 'kilometers', 'price']
            for col in olx_numeric_columns:
                if col in olx_df.columns:
                    olx_df[col] = pd.to_numeric(olx_df[col], errors='coerce')

        return pipeline_df, inventory_df, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df


    def calculate_dealer_match_score(car, dealer_code, historical_df, recent_views_df, recent_filters_df,
                                     dealer_requests_df, olx_df):
        """Calculate how well a car matches a dealer's preferences (for pipeline cars)"""

        total_score = 0
        score_breakdown = {}

        # Get dealer data
        dealer_historical = historical_df[historical_df['dealer_code'] == dealer_code]
        dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_code]
        dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_code]
        dealer_requests = dealer_requests_df[dealer_requests_df['dealer_code'] == dealer_code]
        dealer_olx = olx_df[olx_df['dealer_code'] == dealer_code]

        # 1. Historical Purchase Patterns (40 points total)
        historical_score = 0
        if not dealer_historical.empty:
            # Check for exact model matches first
            model_purchases = dealer_historical[
                (dealer_historical['make'] == car['car_make']) &
                (dealer_historical['model'] == car['car_model'])
                ]

            # Check for make matches (including exact model matches)
            make_purchases = dealer_historical[dealer_historical['make'] == car['car_make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['car_make'])
            group_purchases = pd.DataFrame()
            if car_group != 'Other':
                group_purchases = dealer_historical[
                    dealer_historical['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not model_purchases.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(model_purchases) / len(dealer_historical)
                model_score = min(15, model_frequency * 40)
                historical_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not make_purchases.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not group_purchases.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_purchases) / len(dealer_historical)
                group_score = min(6, group_frequency * 15)
                historical_score += group_score

            # Similar Price Range (4 points) - segment-based scoring
            car_price_segment = get_price_segment(car['sylndr_offer_price'])
            if dealer_historical['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has bought cars in the same price segment
                dealer_price_segments = dealer_historical['price'].apply(get_price_segment)
                price_segment_purchases = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_purchases.empty:
                    price_segment_frequency = len(price_segment_purchases) / len(dealer_historical)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    historical_score += price_segment_score

            # Year Segment preference (3 points) - segment-based scoring
            car_year_segment = get_year_segment(car['car_year'])
            if dealer_historical['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has bought cars in the same year segment
                dealer_year_segments = dealer_historical['year'].apply(get_year_segment)
                year_segment_purchases = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_purchases.empty:
                    # Full score if dealer has bought cars in this year segment
                    historical_score += 3

            # Mileage Segment preference (2 points) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['car_kilometrage'])
            if dealer_historical['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has bought cars in the same mileage segment
                dealer_mileage_segments = dealer_historical['kilometers'].apply(get_mileage_segment)
                mileage_segment_purchases = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_purchases.empty:
                    # Full score if dealer has bought cars in this mileage segment
                    historical_score += 2

        score_breakdown['Historical Purchases'] = historical_score
        total_score += historical_score

        # 2. Recent App Activity (40 points total) - enhanced with additive scoring
        activity_score = 0

        # Recent dealer requests with enhanced matching (20 points max) - Higher priority
        if not dealer_requests.empty:
            # Check for exact model matches first
            exact_model_requests = dealer_requests[
                (dealer_requests['car_make'] == car['car_make']) &
                (dealer_requests['car_model'] == car['car_model'])
                ]

            # Check for make matches (including exact model matches)
            make_requests = dealer_requests[dealer_requests['car_make'] == car['car_make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['car_make'])
            group_requests = pd.DataFrame()
            if car_group != 'Other':
                group_requests = dealer_requests[
                    dealer_requests['car_make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_requests.empty:
                # Exact Model Requests (12 points max)
                model_request_score = min(12, len(exact_model_requests) * 4)
                activity_score += model_request_score

                # Also gets make score (8 points max) since model implies make interest
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score (4 points max) since model implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not make_requests.empty:
                # Make match but no exact model - gets make + origin scores
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score since make implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not group_requests.empty:
                # Only origin group match - gets origin score only
                group_request_score = min(4, len(group_requests) * 0.8)
                activity_score += group_request_score

        # Recent views with enhanced matching (13 points max) - Lower priority
        if not dealer_views.empty:
            # Check for exact model matches first
            exact_model_views = dealer_views[
                (dealer_views['make'] == car['car_make']) &
                (dealer_views['model'] == car['car_model'])
                ]

            # Check for make matches (including exact model matches)
            make_views = dealer_views[dealer_views['make'] == car['car_make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['car_make'])
            group_views = pd.DataFrame()
            if car_group != 'Other':
                group_views = dealer_views[
                    dealer_views['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_views.empty:
                # Exact Model Views (8 points max)
                model_view_score = min(8, len(exact_model_views) * 6)
                activity_score += model_view_score

                # Also gets make score (5 points max) since model implies make interest
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score (2 points max) since model implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not make_views.empty:
                # Make match but no exact model - gets make + origin scores
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score since make implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not group_views.empty:
                # Only origin group match - gets origin score only
                group_view_score = min(2, len(group_views) * 1)
                activity_score += group_view_score

        # Recent filters with enhanced matching (7 points max)
        if not dealer_filters.empty:
            # Check for exact model matches first
            exact_model_filters = dealer_filters[
                (dealer_filters['make'] == car['car_make']) &
                (dealer_filters['model'] == car['car_model'])
                ]

            # Check for make matches (including exact model matches)
            make_filters = dealer_filters[dealer_filters['make'] == car['car_make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['car_make'])
            group_filters = pd.DataFrame()
            if car_group != 'Other':
                group_filters = dealer_filters[
                    dealer_filters['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_filters.empty:
                # Exact Model Filters (4 points max)
                model_filter_score = min(4, len(exact_model_filters) * 2)
                activity_score += model_filter_score

                # Also gets make score (3 points max) since model implies make interest
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score (1 point max) since model implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not make_filters.empty:
                # Make match but no exact model - gets make + origin scores
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score since make implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not group_filters.empty:
                # Only origin group match - gets origin score only
                group_filter_score = min(1, len(group_filters) * 0.5)
                activity_score += group_filter_score

        score_breakdown['Recent Activity'] = activity_score
        total_score += activity_score

        # 3. OLX Listings (40 points total) - updated to match Historical Purchase Patterns structure
        olx_score = 0
        if not dealer_olx.empty:
            # Check for exact model matches first
            exact_model_olx = dealer_olx[
                (dealer_olx['make'] == car['car_make']) &
                (dealer_olx['model'] == car['car_model'])
                ]

            # Check for make matches (including exact model matches)
            make_olx = dealer_olx[dealer_olx['make'] == car['car_make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['car_make'])
            group_olx = pd.DataFrame()
            if car_group != 'Other':
                group_olx = dealer_olx[
                    dealer_olx['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_olx.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(exact_model_olx) / len(dealer_olx)
                model_score = min(15, model_frequency * 40)
                olx_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not make_olx.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not group_olx.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_olx) / len(dealer_olx)
                group_score = min(6, group_frequency * 15)
                olx_score += group_score

            # Similar Price Range (4 points max) - segment-based scoring
            car_price_segment = get_price_segment(car['sylndr_offer_price'])
            if dealer_olx['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has OLX listings in the same price segment
                dealer_price_segments = dealer_olx['price'].apply(get_price_segment)
                price_segment_listings = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_listings.empty:
                    price_segment_frequency = len(price_segment_listings) / len(dealer_olx)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    olx_score += price_segment_score

            # Year Segment preference (3 points max) - segment-based scoring
            car_year_segment = get_year_segment(car['car_year'])
            if dealer_olx['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has OLX listings in the same year segment
                dealer_year_segments = dealer_olx['year'].apply(get_year_segment)
                year_segment_listings = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_listings.empty:
                    # Full score if dealer has listings in this year segment
                    olx_score += 3

            # Mileage Segment preference (2 points max) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['car_kilometrage'])
            if dealer_olx['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has OLX listings in the same mileage segment
                dealer_mileage_segments = dealer_olx['kilometers'].apply(get_mileage_segment)
                mileage_segment_listings = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_listings.empty:
                    # Full score if dealer has listings in this mileage segment
                    olx_score += 2

        score_breakdown['OLX Listings'] = olx_score
        total_score += olx_score

        return total_score, score_breakdown


    def get_top_dealers_for_car(car, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df,
                                top_n=10):
        """Get top matching dealers for a specific car (pipeline cars)"""

        # Get all unique dealers
        all_dealers = set()
        if not historical_df.empty:
            all_dealers.update(historical_df['dealer_code'].unique())
        if not recent_views_df.empty:
            all_dealers.update(recent_views_df['dealer_code'].unique())
        if not recent_filters_df.empty:
            all_dealers.update(recent_filters_df['dealer_code'].unique())
        if not dealer_requests_df.empty:
            all_dealers.update(dealer_requests_df['dealer_code'].unique())
        if not olx_df.empty:
            all_dealers.update(olx_df['dealer_code'].unique())

        dealer_scores = []

        for dealer_code in all_dealers:
            if pd.isna(dealer_code):
                continue

            score, breakdown = calculate_dealer_match_score(
                car, dealer_code, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df
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


    def calculate_inventory_match_score(car, dealer_code, historical_df, recent_views_df, recent_filters_df,
                                        dealer_requests_df, olx_df):
        """Calculate how well an inventory car matches a dealer's preferences"""

        total_score = 0
        score_breakdown = {}

        # Get dealer data
        dealer_historical = historical_df[historical_df['dealer_code'] == dealer_code]
        dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_code]
        dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_code]
        dealer_requests = dealer_requests_df[dealer_requests_df['dealer_code'] == dealer_code]
        dealer_olx = olx_df[olx_df['dealer_code'] == dealer_code]

        # 1. Historical Purchase Patterns (40 points total)
        historical_score = 0
        if not dealer_historical.empty:
            # Check for exact model matches first
            model_purchases = dealer_historical[
                (dealer_historical['make'] == car['make']) &
                (dealer_historical['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_purchases = dealer_historical[dealer_historical['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_purchases = pd.DataFrame()
            if car_group != 'Other':
                group_purchases = dealer_historical[
                    dealer_historical['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not model_purchases.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(model_purchases) / len(dealer_historical)
                model_score = min(15, model_frequency * 40)
                historical_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not make_purchases.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_purchases) / len(dealer_historical)
                make_score = min(10, make_frequency * 25)
                historical_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_purchases.empty:
                    group_frequency = len(group_purchases) / len(dealer_historical)
                    group_score = min(6, group_frequency * 15)
                    historical_score += group_score

            elif not group_purchases.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_purchases) / len(dealer_historical)
                group_score = min(6, group_frequency * 15)
                historical_score += group_score

            # Similar Price Range (4 points) - segment-based scoring
            car_price_segment = get_price_segment(car['App_price'])
            if dealer_historical['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has bought cars in the same price segment
                dealer_price_segments = dealer_historical['price'].apply(get_price_segment)
                price_segment_purchases = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_purchases.empty:
                    price_segment_frequency = len(price_segment_purchases) / len(dealer_historical)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    historical_score += price_segment_score

            # Year Segment preference (3 points) - segment-based scoring
            car_year_segment = get_year_segment(car['year'])
            if dealer_historical['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has bought cars in the same year segment
                dealer_year_segments = dealer_historical['year'].apply(get_year_segment)
                year_segment_purchases = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_purchases.empty:
                    # Full score if dealer has bought cars in this year segment
                    historical_score += 3

            # Mileage Segment preference (2 points) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['kilometers'])
            if dealer_historical['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has bought cars in the same mileage segment
                dealer_mileage_segments = dealer_historical['kilometers'].apply(get_mileage_segment)
                mileage_segment_purchases = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_purchases.empty:
                    # Full score if dealer has bought cars in this mileage segment
                    historical_score += 2

        score_breakdown['Historical Purchases'] = historical_score
        total_score += historical_score

        # 2. Recent App Activity (40 points total) - enhanced with additive scoring
        activity_score = 0

        # Recent dealer requests with enhanced matching (20 points max) - Higher priority
        if not dealer_requests.empty:
            # Check for exact model matches first
            exact_model_requests = dealer_requests[
                (dealer_requests['car_make'] == car['make']) &
                (dealer_requests['car_model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_requests = dealer_requests[dealer_requests['car_make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_requests = pd.DataFrame()
            if car_group != 'Other':
                group_requests = dealer_requests[
                    dealer_requests['car_make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_requests.empty:
                # Exact Model Requests (12 points max)
                model_request_score = min(12, len(exact_model_requests) * 4)
                activity_score += model_request_score

                # Also gets make score (8 points max) since model implies make interest
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score (4 points max) since model implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not make_requests.empty:
                # Make match but no exact model - gets make + origin scores
                make_request_score = min(8, len(make_requests) * 1.5)
                activity_score += make_request_score

                # Also gets origin group score since make implies group interest
                if not group_requests.empty:
                    group_request_score = min(4, len(group_requests) * 0.8)
                    activity_score += group_request_score

            elif not group_requests.empty:
                # Only origin group match - gets origin score only
                group_request_score = min(4, len(group_requests) * 0.8)
                activity_score += group_request_score

        # Recent views with enhanced matching (13 points max) - Lower priority
        if not dealer_views.empty:
            # Check for exact model matches first
            exact_model_views = dealer_views[
                (dealer_views['make'] == car['make']) &
                (dealer_views['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_views = dealer_views[dealer_views['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_views = pd.DataFrame()
            if car_group != 'Other':
                group_views = dealer_views[
                    dealer_views['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_views.empty:
                # Exact Model Views (8 points max)
                model_view_score = min(8, len(exact_model_views) * 6)
                activity_score += model_view_score

                # Also gets make score (5 points max) since model implies make interest
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score (2 points max) since model implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not make_views.empty:
                # Make match but no exact model - gets make + origin scores
                make_view_score = min(5, len(make_views) * 2)
                activity_score += make_view_score

                # Also gets origin group score since make implies group interest
                if not group_views.empty:
                    group_view_score = min(2, len(group_views) * 1)
                    activity_score += group_view_score

            elif not group_views.empty:
                # Only origin group match - gets origin score only
                group_view_score = min(2, len(group_views) * 1)
                activity_score += group_view_score

        # Recent filters with enhanced matching (7 points max)
        if not dealer_filters.empty:
            # Check for exact model matches first
            exact_model_filters = dealer_filters[
                (dealer_filters['make'] == car['make']) &
                (dealer_filters['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_filters = dealer_filters[dealer_filters['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_filters = pd.DataFrame()
            if car_group != 'Other':
                group_filters = dealer_filters[
                    dealer_filters['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_filters.empty:
                # Exact Model Filters (4 points max)
                model_filter_score = min(4, len(exact_model_filters) * 2)
                activity_score += model_filter_score

                # Also gets make score (3 points max) since model implies make interest
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score (1 point max) since model implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not make_filters.empty:
                # Make match but no exact model - gets make + origin scores
                make_filter_score = min(3, len(make_filters) * 1)
                activity_score += make_filter_score

                # Also gets origin group score since make implies group interest
                if not group_filters.empty:
                    group_filter_score = min(1, len(group_filters) * 0.5)
                    activity_score += group_filter_score

            elif not group_filters.empty:
                # Only origin group match - gets origin score only
                group_filter_score = min(1, len(group_filters) * 0.5)
                activity_score += group_filter_score

        score_breakdown['Recent Activity'] = activity_score
        total_score += activity_score

        # 3. OLX Listings (40 points total) - updated to match Historical Purchase Patterns structure
        olx_score = 0
        if not dealer_olx.empty:
            # Check for exact model matches first
            exact_model_olx = dealer_olx[
                (dealer_olx['make'] == car['make']) &
                (dealer_olx['model'] == car['model'])
                ]

            # Check for make matches (including exact model matches)
            make_olx = dealer_olx[dealer_olx['make'] == car['make']]

            # Check for origin group matches (including make and model matches)
            car_group = get_car_group(car['make'])
            group_olx = pd.DataFrame()
            if car_group != 'Other':
                group_olx = dealer_olx[
                    dealer_olx['make'].apply(lambda x: get_car_group(x) == car_group)
                ]

            # Additive scoring - exact model gets all levels
            if not exact_model_olx.empty:
                # Exact Model Match (15 points max)
                model_frequency = len(exact_model_olx) / len(dealer_olx)
                model_score = min(15, model_frequency * 40)
                olx_score += model_score

                # Also gets make score (10 points max) since model implies make interest
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score (6 points max) since model implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not make_olx.empty:
                # Make match but no exact model - gets make + origin scores
                make_frequency = len(make_olx) / len(dealer_olx)
                make_score = min(10, make_frequency * 25)
                olx_score += make_score

                # Also gets origin group score since make implies group interest
                if not group_olx.empty:
                    group_frequency = len(group_olx) / len(dealer_olx)
                    group_score = min(6, group_frequency * 15)
                    olx_score += group_score

            elif not group_olx.empty:
                # Only origin group match - gets origin score only
                group_frequency = len(group_olx) / len(dealer_olx)
                group_score = min(6, group_frequency * 15)
                olx_score += group_score

            # Similar Price Range (4 points max) - segment-based scoring
            car_price_segment = get_price_segment(car['App_price'])
            if dealer_olx['price'].notna().any() and car_price_segment is not None:
                # Check if dealer has OLX listings in the same price segment
                dealer_price_segments = dealer_olx['price'].apply(get_price_segment)
                price_segment_listings = dealer_price_segments[dealer_price_segments == car_price_segment]
                if not price_segment_listings.empty:
                    price_segment_frequency = len(price_segment_listings) / len(dealer_olx)
                    price_segment_score = min(4, price_segment_frequency * 12)
                    olx_score += price_segment_score

            # Year Segment preference (3 points max) - segment-based scoring
            car_year_segment = get_year_segment(car['year'])
            if dealer_olx['year'].notna().any() and car_year_segment is not None:
                # Check if dealer has OLX listings in the same year segment
                dealer_year_segments = dealer_olx['year'].apply(get_year_segment)
                year_segment_listings = dealer_year_segments[dealer_year_segments == car_year_segment]
                if not year_segment_listings.empty:
                    # Full score if dealer has listings in this year segment
                    olx_score += 3

            # Mileage Segment preference (2 points max) - segment-based scoring
            car_mileage_segment = get_mileage_segment(car['kilometers'])
            if dealer_olx['kilometers'].notna().any() and car_mileage_segment is not None:
                # Check if dealer has OLX listings in the same mileage segment
                dealer_mileage_segments = dealer_olx['kilometers'].apply(get_mileage_segment)
                mileage_segment_listings = dealer_mileage_segments[dealer_mileage_segments == car_mileage_segment]
                if not mileage_segment_listings.empty:
                    # Full score if dealer has listings in this mileage segment
                    olx_score += 2

        score_breakdown['OLX Listings'] = olx_score
        total_score += olx_score

        return total_score, score_breakdown


    def get_top_dealers_for_inventory_car(car, historical_df, recent_views_df, recent_filters_df, dealer_requests_df,
                                          olx_df, top_n=10):
        """Get top matching dealers for a specific inventory car"""

        # Get all unique dealers
        all_dealers = set()
        if not historical_df.empty:
            all_dealers.update(historical_df['dealer_code'].unique())
        if not recent_views_df.empty:
            all_dealers.update(recent_views_df['dealer_code'].unique())
        if not recent_filters_df.empty:
            all_dealers.update(recent_filters_df['dealer_code'].unique())
        if not dealer_requests_df.empty:
            all_dealers.update(dealer_requests_df['dealer_code'].unique())
        if not olx_df.empty:
            all_dealers.update(olx_df['dealer_code'].unique())

        dealer_scores = []

        for dealer_code in all_dealers:
            if pd.isna(dealer_code):
                continue

            score, breakdown = calculate_inventory_match_score(
                car, dealer_code, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df
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


    def generate_comprehensive_dealer_matches(pipeline_df, inventory_df, historical_df, recent_views_df,
                                              recent_filters_df, dealer_requests_df, olx_df):
        """Generate comprehensive matches for all dealers with pipeline and inventory cars"""

        # Get all unique dealers
        all_dealers = set()
        if not historical_df.empty:
            all_dealers.update(historical_df['dealer_code'].unique())
        if not recent_views_df.empty:
            all_dealers.update(recent_views_df['dealer_code'].unique())
        if not recent_filters_df.empty:
            all_dealers.update(recent_filters_df['dealer_code'].unique())
        if not dealer_requests_df.empty:
            all_dealers.update(dealer_requests_df['dealer_code'].unique())
        if not olx_df.empty:
            all_dealers.update(olx_df['dealer_code'].unique())

        # Remove any null values
        all_dealers = {dealer for dealer in all_dealers if pd.notna(dealer)}

        comprehensive_matches = []

        # Process each dealer
        for dealer_code in all_dealers:
            # Get dealer name
            dealer_name = "Unknown"
            if not historical_df.empty:
                dealer_info = historical_df[historical_df['dealer_code'] == dealer_code]
                if not dealer_info.empty:
                    dealer_name = dealer_info['dealer_name'].iloc[0]

            # Find top pipeline matches for this dealer
            pipeline_matches = []
            if not pipeline_df.empty:
                for _, car in pipeline_df.head(50).iterrows():  # Limit to top 50 pipeline cars for performance
                    # Check if essential car data is valid
                    if (pd.notna(car['car_make']) and pd.notna(car['car_model']) and
                            pd.notna(car['car_year']) and pd.notna(car['car_kilometrage']) and
                            pd.notna(car['sylndr_offer_price'])):

                        car_for_matching = {
                            'car_make': car['car_make'],
                            'car_model': car['car_model'],
                            'car_year': car['car_year'],
                            'car_kilometrage': car['car_kilometrage'],
                            'sylndr_offer_price': car['sylndr_offer_price']
                        }

                        score, breakdown = calculate_dealer_match_score(
                            car_for_matching, dealer_code, historical_df, recent_views_df, recent_filters_df,
                            dealer_requests_df, olx_df
                        )

                        if score >= 20:  # Only include meaningful matches
                            pipeline_matches.append({
                                'car_id': car['opportunity_code'],
                                'car_name': car['opportunity_name'],
                                'make_model': f"{car['car_make']} {car['car_model']}",
                                'year': car['car_year'],
                                'km': car['car_kilometrage'],
                                'price': car['sylndr_offer_price'],
                                'score': score,
                                'type': 'Pipeline'
                            })

            # Find top inventory matches for this dealer
            inventory_matches = []
            if not inventory_df.empty:
                for _, car in inventory_df.head(50).iterrows():  # Limit to top 50 inventory cars for performance
                    # Check if essential car data is valid
                    if (pd.notna(car['make']) and pd.notna(car['model']) and
                            pd.notna(car['year']) and pd.notna(car['kilometers']) and
                            pd.notna(car['sylndr_offer_price'])):

                        score, breakdown = calculate_inventory_match_score(
                            car, dealer_code, historical_df, recent_views_df, recent_filters_df, dealer_requests_df,
                            olx_df
                        )

                        if score >= 20:  # Only include meaningful matches
                            inventory_matches.append({
                                'car_id': car['sf_vehicle_name'],
                                'car_name': car['sf_vehicle_name'],
                                'make_model': f"{car['make']} {car['model']}",
                                'year': car['year'],
                                'km': car['kilometers'],
                                'price': car['sylndr_offer_price'],
                                'score': score,
                                'type': 'Inventory'
                            })

            # Sort matches by score
            pipeline_matches = sorted(pipeline_matches, key=lambda x: x['score'], reverse=True)[:10]
            inventory_matches = sorted(inventory_matches, key=lambda x: x['score'], reverse=True)[:10]

            # Add to comprehensive matches if dealer has any matches
            if pipeline_matches or inventory_matches:
                comprehensive_matches.append({
                    'dealer_code': dealer_code,
                    'dealer_name': dealer_name,
                    'pipeline_matches': pipeline_matches,
                    'inventory_matches': inventory_matches,
                    'total_pipeline_matches': len(pipeline_matches),
                    'total_inventory_matches': len(inventory_matches),
                    'best_pipeline_score': max([m['score'] for m in pipeline_matches]) if pipeline_matches else 0,
                    'best_inventory_score': max([m['score'] for m in inventory_matches]) if inventory_matches else 0
                })

        # Sort dealers by best scores
        comprehensive_matches = sorted(comprehensive_matches,
                                       key=lambda x: max(x['best_pipeline_score'], x['best_inventory_score']),
                                       reverse=True)

        return comprehensive_matches


    def create_export_dataframe(comprehensive_matches):
        """Create a flattened dataframe suitable for export"""
        export_data = []

        for dealer_match in comprehensive_matches:
            dealer_code = dealer_match['dealer_code']
            dealer_name = dealer_match['dealer_name']

            # Add pipeline matches
            for match in dealer_match['pipeline_matches']:
                export_data.append({
                    'Dealer Code': dealer_code,
                    'Dealer Name': dealer_name,
                    'Car Type': 'Pipeline',
                    'Car ID': match['car_id'],
                    'Car Name': match['car_name'],
                    'Make Model': match['make_model'],
                    'Year': match['year'],
                    'Kilometers': match['km'],
                    'Price (EGP)': match['price'],
                    'Match Score': round(match['score'], 1),
                    'Match Level': '🟢 High' if match['score'] >= 60 else '🟡 Medium' if match[
                                                                                           'score'] >= 40 else '🔴 Low'
                })

            # Add inventory matches
            for match in dealer_match['inventory_matches']:
                export_data.append({
                    'Dealer Code': dealer_code,
                    'Dealer Name': dealer_name,
                    'Car Type': 'Inventory',
                    'Car ID': match['car_id'],
                    'Car Name': match['car_name'],
                    'Make Model': match['make_model'],
                    'Year': match['year'],
                    'Kilometers': match['km'],
                    'Price (EGP)': match['price'],
                    'Match Score': round(match['score'], 1),
                    'Match Level': '🟢 High' if match['score'] >= 60 else '🟡 Medium' if match[
                                                                                           'score'] >= 40 else '🔴 Low'
                })

        return pd.DataFrame(export_data)


    def main():
        st.title("🎯 Pipeline Matcher - Car to Dealer Matching")

        # Add methodology explanation toggle
        show_methodology()

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
            pipeline_df, inventory_df, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df = load_pipeline_data()

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
        tab1, tab2, tab3 = st.tabs(["🎯 Car Matching", "📦 Inventory Matching", "📊 Export"])

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

                # Get matching dealers - fixed function call
                with st.spinner("Finding matching dealers..."):
                    # Create a car dict compatible with the function
                    car_for_matching = {
                        'car_make': selected_car['car_make'],
                        'car_model': selected_car['car_model'],
                        'car_year': selected_car['car_year'],
                        'car_kilometrage': selected_car['car_kilometrage'],
                        'sylndr_offer_price': selected_car['sylndr_offer_price']
                    }

                    top_dealers = get_top_dealers_for_car(
                        car_for_matching, historical_df, recent_views_df, recent_filters_df, dealer_requests_df, olx_df,
                        top_n=15
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
                            'OLX': f"{dealer['score_breakdown'].get('OLX Listings', 0):.1f}",
                            'Car Group': get_car_group(selected_car['car_make'])
                        })

                    dealers_df = pd.DataFrame(dealers_data)

                    # Display with color coding
                    def highlight_score(val):
                        try:
                            score = float(val)
                            if score >= 60:
                                return 'background-color: #90EE90'  # Light green
                            elif score >= 40:
                                return 'background-color: #FFE4B5'  # Light orange
                            elif score >= 20:
                                return 'background-color: #FFCCCB'  # Light red
                            else:
                                return ''
                        except:
                            return ''

                    st.dataframe(
                        dealers_df.style.applymap(highlight_score, subset=['Match Score']),
                        use_container_width=True
                    )

                    st.info("🟢 High Match (60+) | 🟡 Medium Match (40-59) | 🔴 Low Match (20-39)")

                    # Show car group analysis
                    car_group = get_car_group(selected_car['car_make'])
                    st.write(f"**Selected Car Group:** {car_group} ({selected_car['car_make']})")

                    # Detailed analysis for top 3 dealers
                    st.subheader("📋 Detailed Analysis - Top 3 Dealers")

                    for i, dealer in enumerate(top_dealers[:3]):
                        with st.expander(f"{i + 1}. {dealer['dealer_name']} (Score: {dealer['match_score']:.1f})"):
                            col1, col2 = st.columns(2)

                            with col1:
                                st.write("**Score Breakdown:**")
                                for component, score in dealer['score_breakdown'].items():
                                    st.write(f"• {component}: {score:.1f} points")

                            with col2:
                                # Show relevant dealer activity with grouping info
                                dealer_hist = historical_df[historical_df['dealer_code'] == dealer['dealer_code']]
                                dealer_olx_cars = olx_df[olx_df['dealer_code'] == dealer['dealer_code']]

                                if not dealer_hist.empty:
                                    st.write("**Recent Purchases:**")
                                    recent_purchases = dealer_hist.head(3)
                                    for _, purchase in recent_purchases.iterrows():
                                        purchase_group = get_car_group(purchase['make'])
                                        match_indicator = "🎯" if purchase['make'] == selected_car['car_make'] and \
                                                                 purchase['model'] == selected_car['car_model'] else \
                                            "✅" if purchase['make'] == selected_car['car_make'] else \
                                                "🔄" if purchase_group == car_group else "•"
                                        st.write(
                                            f"{match_indicator} {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")

                                if not dealer_olx_cars.empty:
                                    st.write("**OLX Listings:**")
                                    recent_olx = dealer_olx_cars.head(3)
                                    for _, listing in recent_olx.iterrows():
                                        listing_group = get_car_group(listing['make'])
                                        match_indicator = "🎯" if listing['make'] == selected_car['car_make'] and \
                                                                 listing['model'] == selected_car['car_model'] else \
                                            "✅" if listing['make'] == selected_car['car_make'] else \
                                                "🔄" if listing_group == car_group else "•"
                                        st.write(
                                            f"{match_indicator} {listing['make']} {listing['model']} ({listing['year']}) - EGP {listing['price']:,.0f}")

                            # Add legend for symbols
                            st.write("**Legend:** 🎯 Exact Model | ✅ Same Make | 🔄 Same Group | • Other")

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
                                selected_car, historical_df, recent_views_df, recent_filters_df, dealer_requests_df,
                                olx_df, top_n=15
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
                                    'OLX': f"{dealer['score_breakdown'].get('OLX Listings', 0):.1f}",
                                    'Car Group': get_car_group(selected_car['make'])
                                })

                            dealers_df = pd.DataFrame(dealers_data)

                            # Display with color coding
                            def highlight_score(val):
                                try:
                                    score = float(val)
                                    if score >= 60:
                                        return 'background-color: #90EE90'  # Light green
                                    elif score >= 40:
                                        return 'background-color: #FFE4B5'  # Light orange
                                    elif score >= 20:
                                        return 'background-color: #FFCCCB'  # Light red
                                    else:
                                        return ''
                                except:
                                    return ''

                            st.dataframe(
                                dealers_df.style.applymap(highlight_score, subset=['Interest Score']),
                                use_container_width=True
                            )

                            st.info("🟢 High Interest (60+) | 🟡 Medium Interest (40-59) | 🔴 Low Interest (20-39)")

                            # Show top 3 dealers analysis
                            st.subheader("📋 Top 3 Most Interested Dealers")

                            for i, dealer in enumerate(top_dealers[:3]):
                                with st.expander(
                                        f"{i + 1}. {dealer['dealer_name']} (Score: {dealer['match_score']:.1f})"):
                                    col1, col2 = st.columns(2)

                                    with col1:
                                        st.write("**Interest Breakdown:**")
                                        for component, score in dealer['score_breakdown'].items():
                                            st.write(f"• {component}: {score:.1f} points")

                                    with col2:
                                        # Show relevant dealer activity with grouping info
                                        dealer_hist = historical_df[
                                            historical_df['dealer_code'] == dealer['dealer_code']]
                                        dealer_olx_cars = olx_df[olx_df['dealer_code'] == dealer['dealer_code']]

                                        if not dealer_hist.empty:
                                            st.write("**Recent Purchases:**")
                                            similar_purchases = dealer_hist[
                                                (dealer_hist['make'] == selected_car['make']) |
                                                (dealer_hist['model'] == selected_car['model'])
                                                ].head(3)

                                            if not similar_purchases.empty:
                                                for _, purchase in similar_purchases.iterrows():
                                                    purchase_group = get_car_group(purchase['make'])
                                                    match_indicator = "🎯" if purchase['make'] == selected_car[
                                                        'make'] and purchase['model'] == selected_car['model'] else \
                                                        "✅" if purchase['make'] == selected_car['make'] else \
                                                            "🔄" if purchase_group == get_car_group(
                                                                selected_car['make']) else "•"
                                                    st.write(
                                                        f"{match_indicator} {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")
                                            else:
                                                # Show any recent purchases
                                                recent_purchases = dealer_hist.head(2)
                                                for _, purchase in recent_purchases.iterrows():
                                                    st.write(
                                                        f"• {purchase['make']} {purchase['model']} ({purchase['year']}) - EGP {purchase['price']:,.0f}")

                                        if not dealer_olx_cars.empty:
                                            st.write("**OLX Listings:**")
                                            similar_olx = dealer_olx_cars[
                                                (dealer_olx_cars['make'] == selected_car['make']) |
                                                (dealer_olx_cars['model'] == selected_car['model'])
                                                ].head(3)

                                            if not similar_olx.empty:
                                                for _, listing in similar_olx.iterrows():
                                                    listing_group = get_car_group(listing['make'])
                                                    match_indicator = "🎯" if listing['make'] == selected_car['make'] and \
                                                                             listing['model'] == selected_car[
                                                                                 'model'] else \
                                                        "✅" if listing['make'] == selected_car['make'] else \
                                                            "🔄" if listing_group == get_car_group(
                                                                selected_car['make']) else "•"
                                                    st.write(
                                                        f"{match_indicator} {listing['make']} {listing['model']} ({listing['year']}) - EGP {listing['price']:,.0f}")

                        else:
                            st.warning("No interested dealers found for this car.")
                else:
                    st.info("No cars available in inventory.")

        with tab3:
            st.subheader("📊 Comprehensive Dealer-Car Export")

            # Track tab view
            if "current_user" in st.session_state:
                posthog.capture(
                    st.session_state["current_user"],
                    'tab_view',
                    {
                        'tab': 'export',
                        'timestamp': datetime.now().isoformat()
                    }
                )

            st.info("🔄 Generating comprehensive matches for all dealers... This may take a moment.")

            # Generate comprehensive matches
            with st.spinner("Analyzing all dealer-car matches..."):
                comprehensive_matches = generate_comprehensive_dealer_matches(
                    pipeline_df, inventory_df, historical_df, recent_views_df, recent_filters_df, dealer_requests_df,
                    olx_df
                )

            if comprehensive_matches:
                # Display summary metrics
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    total_dealers = len(comprehensive_matches)
                    st.metric("Dealers with Matches", total_dealers)

                with col2:
                    total_pipeline_matches = sum([d['total_pipeline_matches'] for d in comprehensive_matches])
                    st.metric("Total Pipeline Matches", total_pipeline_matches)

                with col3:
                    total_inventory_matches = sum([d['total_inventory_matches'] for d in comprehensive_matches])
                    st.metric("Total Inventory Matches", total_inventory_matches)

                with col4:
                    avg_score = sum([max(d['best_pipeline_score'], d['best_inventory_score']) for d in
                                     comprehensive_matches]) / len(comprehensive_matches)
                    st.metric("Avg Best Score", f"{avg_score:.1f}")

                # Create export dataframe
                export_df = create_export_dataframe(comprehensive_matches)

                # Export functionality
                st.subheader("📥 Export Options")

                col1, col2 = st.columns(2)

                with col1:
                    # Download as CSV
                    csv_data = export_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download as CSV",
                        data=csv_data,
                        file_name=f"dealer_car_matches_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
                        mime="text/csv"
                    )

                with col2:
                    # Filter options
                    min_score = st.slider("Minimum Match Score", 0, 100, 20, 5)
                    filtered_export_df = export_df[export_df['Match Score'] >= min_score]

                # Display filtered results
                st.subheader("🎯 Dealer-Car Matches Overview")

                # Summary by dealer
                st.write("**Top Dealers by Match Quality:**")
                dealer_summary = []
                for dealer_match in comprehensive_matches[:20]:  # Show top 20 dealers
                    dealer_summary.append({
                        'Dealer Code': dealer_match['dealer_code'],
                        'Dealer Name': dealer_match['dealer_name'],
                        'Pipeline Matches': dealer_match['total_pipeline_matches'],
                        'Inventory Matches': dealer_match['total_inventory_matches'],
                        'Best Pipeline Score': f"{dealer_match['best_pipeline_score']:.1f}",
                        'Best Inventory Score': f"{dealer_match['best_inventory_score']:.1f}",
                        'Overall Best': f"{max(dealer_match['best_pipeline_score'], dealer_match['best_inventory_score']):.1f}"
                    })

                dealer_summary_df = pd.DataFrame(dealer_summary)
                st.dataframe(dealer_summary_df, use_container_width=True)

                # Detailed matches view
                st.subheader("🔍 Detailed Matches")

                # Filter by dealer
                selected_dealer_export = st.selectbox(
                    "Select dealer to view detailed matches:",
                    options=["All Dealers"] + [d['dealer_name'] for d in comprehensive_matches],
                    key="export_dealer_select"
                )

                if selected_dealer_export == "All Dealers":
                    display_df = filtered_export_df
                else:
                    display_df = filtered_export_df[filtered_export_df['Dealer Name'] == selected_dealer_export]

                # Add color coding function
                def highlight_match_level(row):
                    if row['Match Level'] == '🟢 High':
                        return ['background-color: #90EE90'] * len(row)
                    elif row['Match Level'] == '🟡 Medium':
                        return ['background-color: #FFE4B5'] * len(row)
                    elif row['Match Level'] == '🔴 Low':
                        return ['background-color: #FFCCCB'] * len(row)
                    else:
                        return [''] * len(row)

                if not display_df.empty:
                    st.dataframe(
                        display_df.style.apply(highlight_match_level, axis=1),
                        column_config={
                            "Kilometers": st.column_config.NumberColumn(
                                "Kilometers",
                                format="%d km"
                            ),
                            "Price (EGP)": st.column_config.NumberColumn(
                                "Price (EGP)",
                                format="EGP %d"
                            )
                        },
                        use_container_width=True
                    )

                    st.info(f"Showing {len(display_df)} matches | 🟢 High: 60+ | 🟡 Medium: 40-59 | 🔴 Low: 20-39")
                else:
                    st.warning("No matches found with the current filters.")

                # Individual dealer details
                if selected_dealer_export != "All Dealers":
                    selected_dealer_data = next(
                        (d for d in comprehensive_matches if d['dealer_name'] == selected_dealer_export), None)

                    if selected_dealer_data:
                        st.subheader(f"📋 {selected_dealer_export} - Detailed Breakdown")

                        col1, col2 = st.columns(2)

                        with col1:
                            st.write("**🎯 Top Pipeline Matches:**")
                            if selected_dealer_data['pipeline_matches']:
                                for i, match in enumerate(selected_dealer_data['pipeline_matches'][:5], 1):
                                    score_color = "🟢" if match['score'] >= 60 else "🟡" if match['score'] >= 40 else "🔴"
                                    st.write(
                                        f"{i}. {score_color} {match['make_model']} ({match['year']}) - Score: {match['score']:.1f}")
                            else:
                                st.write("No pipeline matches")

                        with col2:
                            st.write("**📦 Top Inventory Matches:**")
                            if selected_dealer_data['inventory_matches']:
                                for i, match in enumerate(selected_dealer_data['inventory_matches'][:5], 1):
                                    score_color = "🟢" if match['score'] >= 60 else "🟡" if match['score'] >= 40 else "🔴"
                                    st.write(
                                        f"{i}. {score_color} {match['make_model']} ({match['year']}) - Score: {match['score']:.1f}")
                            else:
                                st.write("No inventory matches")
            else:
                st.warning("No dealer matches found. This could be due to limited data or high scoring thresholds.")


    if __name__ == "__main__":
        main()
