# Tableau Calculated Fields

## 1. Total_Waste
**Formula:** `SUM([Waste Score])`

**Purpose:** Total waste across all resources

**Used in:** Executive Dashboard KPIs

## 2. Savings_Potential
**Formula:** `SUM(IF [Waste Category] = "Critical" THEN [Monthly Waste] END)`

**Purpose:** Potential savings from Critical waste only

**Used in:** ROI calculations

## 3. ROI_Ratio
**Formula:** `SUM([Monthly Waste]) / (SUM([Cost]) * 30)`

**Purpose:** Waste as percentage of total spend

**Format:** Percentage with 1 decimal

**Used in:** Executive summary headline

## 4. Waste_Category_Color
**Formula:** Color codes for waste priorities

**Colors:**

- Critical: #DC3545 (Red)
- High: #FD7E14 (Orange)
- Medium: #FFC107 (Yellow)
- Low: #28A745 (Green)

## 5. Monthly_Projection
**Formula:** `SUM([Cost]) * 30`

**Purpose:** Forecast monthly spend based on current daily rate

**Used in:** Budget comparison
