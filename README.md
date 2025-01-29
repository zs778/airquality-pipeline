# airquality-pipeline
In this project, I build a data pipeline to process air quality data and then visualize it in a dashboard. First, I extracted JSON files from a government data website. They are then ingested into Snowflake's raw layer. We then copy JSON data into Snowflake's tables, clean and transform them, design star schema models and then finally create a dashboard using Streamlit within Snowflake.

Screenshot of dashboard:

<img width="215" alt="Screenshot 2025-01-29 172758" src="https://github.com/user-attachments/assets/a3a4fc8b-55da-4600-abcc-a9299955f17e" />
<img width="208" alt="Screenshot 2025-01-29 172829" src="https://github.com/user-attachments/assets/166fdc03-969b-4757-9451-cc87c83578eb" />
