import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import logging
from concurrent.futures import ThreadPoolExecutor
import time
import numpy as np
import os

# Set up logging
os.makedirs('data', exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=os.path.join('data', 'stock_data_collection.log')
)

# Dictionary mapping ticker symbols to airline codes and names
AIRLINE_MAPPING = {
    'AAL': {'name': 'American Airlines Inc.', 'code': 'AA'},
    'DAL': {'name': 'Delta Air Lines Inc.', 'code': 'DL'},
    'LUV': {'name': 'Southwest Airlines Co.', 'code': 'WN'},
    'UAL': {'name': 'United Air Lines Inc.', 'code': 'UA'},
    'ATSG': {'name': 'Air Transport International', 'code': '8C'},
    'ALK': {'name': 'Alaska Airlines Inc.', 'code': 'AS'},
    'ALGT': {'name': 'Allegiant Air', 'code': 'G4'},
    'HA': {'name': 'Hawaiian Airlines Inc.', 'code': 'HA'},
    'ULCC': {'name': 'Frontier Airlines Inc.', 'code': 'F9'},
    'JBLU': {'name': 'JetBlue Airways', 'code': 'B6'},
    'MESA': {'name': 'Mesa Airlines Inc.', 'code': 'YV'},
    'SKYW': {'name': 'SkyWest Airlines Inc.', 'code': 'OO'},
    'SAVE': {'name': 'Spirit Air Lines', 'code': 'NK'},
    'SNCY': {'name': 'Sun Country Airlines d/b/a MN Airlines', 'code': 'SY'}
}

def get_stock_data(ticker, start_date, end_date, retries=3, delay=2):
    """
    Get stock data with retry mechanism and error handling
    """
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            data = stock.history(start=start_date, end=end_date)
            
            if not data.empty:
                # Get additional metrics
                data['DailyReturn'] = data['Close'].pct_change()
                data['Volume_MA_5'] = data['Volume'].rolling(window=5).mean()
                data['Volatility'] = data['DailyReturn'].rolling(window=20).std()
                
                return data
            
            time.sleep(delay)  # Prevent rate limiting
            
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {ticker}: {str(e)}")
            if attempt == retries - 1:
                logging.error(f"All attempts failed for {ticker}")
                return pd.DataFrame()
            time.sleep(delay * (attempt + 1))  # Exponential backoff
    
    return pd.DataFrame()

def calculate_quarterly_metrics(data):
    """
    Calculate comprehensive quarterly metrics
    """
    if data.empty:
        return pd.DataFrame()
    
    quarterly = pd.DataFrame()
    quarterly['Average_Price'] = data['Close'].resample('QE').mean()
    quarterly['End_Price'] = data['Close'].resample('QE').last()
    quarterly['Start_Price'] = data['Close'].resample('QE').first()
    quarterly['High_Price'] = data['High'].resample('QE').max()
    quarterly['Low_Price'] = data['Low'].resample('QE').min()
    quarterly['Volume'] = data['Volume'].resample('QE').mean()
    quarterly['Volatility'] = data['Volatility'].resample('QE').mean()
    quarterly['Return'] = quarterly['End_Price'].pct_change()
    
    # Add quarter and year
    quarterly['YEAR'] = quarterly.index.year
    quarterly['QUARTER'] = quarterly.index.quarter
    
    return quarterly

def process_ticker(ticker):
    """
    Process individual ticker with error handling
    """
    try:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365*34)
        
        logging.info(f"Processing {ticker}")
        
        stock_data = get_stock_data(ticker, start_date, end_date)
        if stock_data.empty:
            logging.warning(f"No data retrieved for {ticker}")
            return None
        
        quarterly_data = calculate_quarterly_metrics(stock_data)
        if quarterly_data.empty:
            return None
            
        # Add ticker and airline information
        quarterly_data['Ticker'] = ticker
        quarterly_data['UNIQUE_CARRIER_NAME'] = AIRLINE_MAPPING[ticker]['name']
        quarterly_data['UNIQUE_CARRIER'] = AIRLINE_MAPPING[ticker]['code']
        
        return quarterly_data
        
    except Exception as e:
        logging.error(f"Error processing {ticker}: {str(e)}")
        return None

def main():
    """
    Main function with parallel processing
    """
    try:
        logging.info("Starting stock data collection")
        
        # Create data directory if it doesn't exist
        data_dir = 'data'
        os.makedirs(data_dir, exist_ok=True)
        
        all_data = []
        
        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(process_ticker, AIRLINE_MAPPING.keys())
            
            for result in results:
                if result is not None:
                    all_data.append(result)
        
        if not all_data:
            logging.error("No data collected for any ticker")
            return
        
        # Combine all data
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Clean and format data
        final_df = final_df.round(2)
        final_df = final_df.sort_values(['Ticker', 'YEAR', 'QUARTER'])
        
        # Save to CSV in data directory
        output_file = os.path.join(data_dir, 'airline_stock_data.csv')
        final_df.to_csv(output_file, index=False)
        logging.info(f"Data saved to {output_file}")
        
        # Print summary statistics
        print("\nCollection Summary:")
        print(f"Total airlines processed: {len(final_df['Ticker'].unique())}")
        print(f"Date range: {final_df['YEAR'].min()}-Q{final_df['QUARTER'].min()} to {final_df['YEAR'].max()}-Q{final_df['QUARTER'].max()}")
        print(f"Total records: {len(final_df)}")
        
    except Exception as e:
        logging.error(f"Main process error: {str(e)}")

if __name__ == "__main__":
    main()
