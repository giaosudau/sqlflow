-- Load and clean contact data from Google Sheets
CREATE TABLE clean_contacts AS
SELECT 
    TRIM(name) as contact_name,
    LOWER(TRIM(email)) as email_address,
    TRIM(phone) as phone_number,
    TRIM(company) as company_name,
    CASE 
        WHEN LOWER(TRIM(status)) IN ('active', 'a') THEN 'Active'
        WHEN LOWER(TRIM(status)) IN ('inactive', 'i') THEN 'Inactive'
        ELSE 'Unknown'
    END as contact_status,
    CURRENT_TIMESTAMP as processed_at
FROM google_sheets_contacts
WHERE email IS NOT NULL 
  AND email != ''
  AND name IS NOT NULL 
  AND name != ''; 