# 1️⃣📊 Real-Time Crypto Market Risk Monitoring Using Live Market Data


#                      _End-to-End Financial Risk Analysis Using Python, SQL & Power BI_

## 2️⃣ Executive Summary

This project builds a real-time market risk monitoring system using live crypto market data of 4 assets (Bitcoin (BTC), Ethereum (ETH), Binance Coin (BNB), Solana (SOL)). The objective is to assess market stability, downside exposure, liquidity conditions, and diversification effectiveness through quantitative risk metrics. 

Using rolling volatility, drawdown analysis, and correlation modeling, the system identifies systemic risk-off regimes and asset-level risk amplification patterns.

#### Key findings include:

- Volatility spikes precede peak drawdowns

- Diversification benefits weaken during stress

- Bitcoin demonstrates relative resilience

- Ethereum and Binance Coin amplify downside risk

This project aimed to design an analytical pipeline by applying statistical methods and communicating the financial insights thereforth.

## 3️⃣ Business Problem

Financial markets experience periods of elevated volatility and systemic stress. Risk managers require tools that:

    - Detect rising market risk early
    
    - Quantify downside exposure
    
    - Monitor liquidity stability
    
    - Evaluate diversification effectiveness

Raw market prices alone are insufficient. The challenge is transforming raw time-series data into actionable risk intelligence.

This analysis is explicitly designed to answer questions executives actually ask:

    1. Is market risk increasing or stabilizing?
    
    2. How severe could downside losses be?
    
    3. Is liquidity sufficient to support orderly exits?
    
    4. Are assets behaving independently or moving together?
    
    5. Do current conditions resemble a risk-off regime?

## 4️⃣ Methodology
### 🔎 Architecture Overview

<img width="252" height="488" alt="image" src="https://github.com/user-attachments/assets/360bc47c-7c15-4cbd-8aad-05a28618a53d" />

_Step 1: Data Ingestion_

	 i-  Live market data retrieved via CoinGecko API

	ii-  5-minute ingestion interval

    iii- Append-only storage for durability

    iv-  UTC timestamping for consistency

##### (API calls were scheduled for the sweetspot of 5 minutes given free plan call limits)
This mirrors real production trade-offs between data freshness and cost. (within quota, Sustainable, Adequate granularity for risk monitoring)

    - Each API call returns a market snapshot
    
    - Snapshots are time-stamped
    
    - Data is appended, not overwritten
    
    - Repeated calls construct a true time series
    
    - Time series are built through accumulation, not single API calls.

_Step 2: SQL Data Modeling_

      i-  CTEs for modular transformations

     ii-  Aggregate functions for volume and volatility

    iii-  CASE statements for risk regime labeling

     iv-  Rolling calculations using window functions

      v-  Asset-level joins for consolidated analysis

_Step 3: Statistical Feature Engineering (Python)_

      i- Using Pandas and NumPy:

     ii- Percentage returns

    iii- Winsorization for outlier handling

     iv- Z-score normalization

      v- Rolling standard deviation

     vi- Correlation matrix construction

    vii- Cumulative maximum & drawdown calculations

_Step 4: Visualization & BI Layer (Power BI)_

     i-  Data modeling relationships

    ii-  DAX measures for volatility and risk KPIs

    iii- Calculated columns for regime tagging

    iv-  Interactive dashboards

     v-  Liquidity vs volatility analysis

    vi-  Drawdown trend monitoring

## 5️⃣ Skills
🔹 SQL
    CTEs, Joins, CASE logic, Aggregate functions, Window functions, Time-series modeling

🔹 Power BI
    DAX measures, Calculated columns, ETL with Power Query, Data modeling, KPI dashboards, Risk visualization

🔹 Python
    Pandas, NumPy, Matplotlib / Plotly, Rolling statistics, Function modularization, Statistical normalization

🔹 Financial Analytics
    Market risk modeling, Volatility analysis, Drawdown measurement, Liquidity assessment, Correlation & diversification 
    analysis, Risk regime identification

## 6️⃣ Results & Business Recommendations
### Key Findings

    - Rising volatility acts as a leading indicator of drawdowns

    - Correlations increase during stress, reducing diversification

    - Ethereum and Binance Coin show higher downside sensitivity

    - Liquidity remained stable during observed stress events

### Business Recommendations (Next Steps)

    - Implement volatility threshold alerts

    - Monitor correlation spikes as diversification risk signals

    - Assess asset-level risk contribution during stress periods

    - Incorporate rolling volatility into portfolio monitoring frameworks

### ⚠️ Limitations

    - Limited Crypto-only asset scope

    - Short intraday observation window

    - Exchange-aggregated market data

### 🚀 Future Enhancements

    - PostgreSQL data storage

    - Automated alerting system

    - Value-at-Risk modeling

    - Stress testing scenarios

	
### 👤 Author

#### Ken Mwangi 
Data Engineer | Machine Learning Engineer | AWS Certified | Data Analyst

Portfolio website: https://KenMwangi1.github.io/

LinkedIn: https://www.linkedin.com/in/ken-mwangi-81478028/
