import streamlit as st
import pandas as pd
import joblib

# Load the trained model and scaler
model = joblib.load('xgb_model.pkl')
scaler = joblib.load('scaler.pkl')

# Define the features selected by RFE
selected_features = [
    'LPC_outlet_temperature (T24)', 'fan_inlet_pressure (P2)',
    'bypass_duct_pressure (P15)', 'HPC_outlet_static_pressure (Ps30)',
    'mach_number (mach)', 'throttle_resolver_angle (TRA)',
    'fan_inlet_temperature (T2)', 'engine_pressure_ratio (epr)',
    'corrected_fan_speed (NRf)', 'bypass_ratio (BPR)',
    'burner_fuel_air_ratio (farB)', 'demanded_fan_speed (Nf_dmd)',
    'demanded_corrected_fan_speed (PCNfR_dmd)', 'HPT_coolant_bleed (W31)',
    'LPT_coolant_bleed (W32)'
]

# Function to scale features and predict RUL
def predict_rul(input_features):
    input_df = pd.DataFrame([input_features], columns=selected_features)
    scaled_features = scaler.transform(input_df)
    prediction = model.predict(scaled_features)
    return prediction[0]

# Streamlit app UI
st.title("Turbofan Engine RUL Prediction")
st.markdown("---")

st.markdown("### Enter the following engine parameters to predict the Remaining Useful Life (RUL):")

input_features = {}

for feature in selected_features:
    value = st.text_input(f"{feature}", key=feature)
    input_features[feature] = value.strip() if value else None  # Store as None if blank

st.markdown("---")

if st.button("Predict RUL"):
    if any(value == "" or value is None for value in input_features.values()):
        st.error("Please enter valid numbers for all the features.")
    else:
        try:
            input_features = {k: float(v) for k, v in input_features.items()}
            rul_prediction = predict_rul(input_features)
            st.success(f"### Predicted Remaining Useful Life (RUL): {rul_prediction:.0f} cycles")
        except ValueError:
            st.error("Please enter valid numbers for all the features.")



