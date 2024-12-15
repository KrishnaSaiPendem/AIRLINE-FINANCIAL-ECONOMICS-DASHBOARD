# Airline Financial Dashboard

This repository contains a web application that provides an in-depth analysis of the financial health and market performance of major U.S. airlines. The application combines an interactive React-based frontend with a Python Flask backend that utilizes machine learning models for stock price prediction.

## Features

### Frontend (React)
- **Interactive Dashboards:** Provides dynamic visualizations using Tableau dashboards.
- **Individual Airline Analysis:** View airline-specific data including historical stock prices, comparisons, financials, and forecast.
- **User-Friendly Interface:** Responsive design for seamless interaction.

### Backend (Flask + Machine Learning)
- **Stock Price Prediction:** Predicts future stock prices using LSTM, ARIMA, and GRU models.
- **Real-Time Insights:** Processes airline financial and operational metrics from Form 41 data.
- **Customizable Predictions:** Users can select an airline and forecast its stock performance.

### Data Source
- **Form 41 Data:** Airline financial and operational data maintained by the Bureau of Transportation Statistics (BTS).

---

## Getting Started

### Prerequisites
- **Node.js** (for React frontend)
- **Python 3.8+** (for Flask backend)
- **AWS Account** (for hosting)
- Tableau dashboards should be embedded and accessible publicly.

---

### Local Setup

#### 1. Backend (Flask)
1. Navigate to the backend folder:
    ```bash
    cd backend
    ```
2. Install Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. Start the Flask server:
    ```bash
    python prediction.py
    ```
4. The backend will be running at `http://127.0.0.1:5000`.

#### 2. Frontend (React)
1. Navigate to the frontend folder:
    ```bash
    cd frontend
    ```
2. Install Node.js dependencies:
    ```bash
    npm install
    ```
3. Start the React app:
    ```bash
    npm start
    ```
4. The frontend will be running at `http://localhost:3000`.

---

## Deployment

### AWS Deployment

#### 1. Backend (Flask)
1. **Create an Elastic Beanstalk Application**:
   - Upload the backend folder zipped as a Python project.
   - Use **Python 3.12 running on Amazon Linux 2023**.
   
2. **Setup Environment Variables**:
   - Configure any necessary API keys, database URLs, or settings.

3. **Deploy the Application**:
   - Access the backend API endpoint via the provided AWS URL.

#### 2. Frontend (React)
1. **Build the React Application**:
    ```bash
    npm run build
    ```
2. **Upload to S3 Bucket**:
   - Create an S3 bucket and enable static website hosting.
   - Upload the contents of the `build` folder to the bucket.
3. **Configure CloudFront (Optional)**:
   - Use AWS CloudFront for caching and global distribution.

---

## Project Sections

### 1. Overview
- Historical stock price visualization with filters for specific years and quarters.
- Key metrics such as date, closing price, and trading volume.

### 2. Compare
- Compare the selected airline's stock price with other airlines.
- Analyze stock performance trends across the industry.

### 3. Financials
- Interactive dashboards for revenue, operating margins, net income, and debt-to-equity analysis.

### 4. Forecast
- Predict stock prices for the next three quarters using machine learning models.
- Accessible predictions based on Form 41 financial data.

---

## Technologies Used

### Frontend
- **React.js** for dynamic user interfaces.
- **Tableau** for embedded interactive dashboards.

### Backend
- **Flask** for API and server-side logic.
- **Python** for data processing and machine learning (LSTM, ARIMA, GRU).

---

## Future Improvements
- **Enhanced Forecast Models:** Incorporate additional metrics for more accurate predictions.
- **User Authentication:** Allow user-specific dashboards and saved preferences.
- **Real-Time Updates:** Include live stock price updates via APIs.

---

## License
This project is licensed under the MIT License.

---

## Contributors
- [Vishal Orsu](https://github.com/vishalorsu)
- [Adarsh Gorremuchu](https://github.com/AdarshGorremuchu)

---

## Acknowledgments
- **Bureau of Transportation Statistics (BTS):** For providing Form 41 data.
- **Tableau Public:** For hosting and embedding dashboards.
- **AWS:** For robust cloud hosting solutions.
