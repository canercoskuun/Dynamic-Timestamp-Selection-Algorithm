⚡ Dynamic Timestamp Validation with Apache Spark

This project implements a **Dynamic Timestamp Validation Algorithm** based on a decision flowchart. It processes message timestamps using Apache Spark and classifies whether a message should be **processed** or **skipped**, based on a set of timestamp consistency rules.

## 🔍 Overview

The logic used is derived from the decision flowchart below:

## Algorithm



![Algorithm](https://github.com/user-attachments/assets/fbb3c1fd-8767-4e62-bc21-3685e197e0d2)


We use three timestamp columns from the dataset:

| Field in Dataset         | Mapped to             |
|--------------------------|-----------------------|
| `signal_timestampt`      | `internalRTCTime`     |
| `signal_oem_timestmap`   | `modemUTCDateTime`    |
| `ts`                     | `ingestionTime`       |

## 📁 Dataset

The dummy dataset provided (`dummy_data.csv`) contains example records with various timestamp combinations to validate the flowchart rules.

## 🧠 Algorithm Logic

Each row in the dataset is classified as either:
- ✅ **Message Processed**
- ❌ **Message Not Processed**

The decision is based on conditions such as:
- Missing or invalid timestamps
- Recognized faulty years (e.g., 1970, March 2021)
- Future or past timestamp range checks
- Valid millisecond resolution
