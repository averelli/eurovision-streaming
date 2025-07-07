import streamlit as st
import plotly.express as px
import pandas as pd
from processors.dashboard_consumer import consume_windows

WINDOW_COLUMNS = ["id", "window_start", "window_posts", "window_end", "total_posts", "window_sentiment", "total_sentiment", "window_avg_sentiment", "total_avg_sentiment", "window_platform_counter", "total_platform_counter", "window_top_song", "window_top_country", "window_top_contestant", "total_top_song", "total_top_country", "total_top_contestant", "event_label"]

placeholder = st.empty()
df = pd.DataFrame(columns=WINDOW_COLUMNS)

for win in consume_windows(pause_time=3):
    event_label = win["event_label"]
    win_df = pd.DataFrame([win], columns=WINDOW_COLUMNS).replace("none", None)
    df = pd.concat([df, win_df], ignore_index=True)

    with placeholder:
        with st.container() as dashboard:
        
            # top row metrics: total and window stats | total sentiment
            top_stats, total_sent = st.columns([3, 1])

            # table of top song, country, contestant in total and window
            with top_stats:
                # Headers
                top_labels = ["** **", "**SONG**", "**COUNTRY**", "**CONTESTANT**"]
                headers = st.columns(4)
                for i, label in enumerate(top_labels):
                    headers[i].markdown(label)

                # second row - total top
                total_top_song = win.get("total_top_song")
                total_top_country = win.get("total_top_country")
                total_top_contestant = win.get("total_top_contestant")
                
                second_row = st.columns(4)
                second_row[0].markdown("Total:")
                second_row[1].markdown(total_top_song[0] if total_top_song else "NONE")
                second_row[2].markdown(total_top_country[0] if total_top_country else "NONE")
                second_row[3].markdown(total_top_contestant[0] if total_top_contestant else "NONE")

                # third row - window top
                window_top_song = win.get("window_top_song")
                window_top_country = win.get("window_top_country")
                window_top_contestant = win.get("window_top_contestant")
                
                third_row = st.columns(4)
                third_row[1].markdown(window_top_song[0] if window_top_song else "NONE")
                third_row[0].markdown("Window:")
                third_row[2].markdown(window_top_country[0] if window_top_country else "NONE")
                third_row[3].markdown(window_top_contestant[0] if window_top_contestant else "NONE")

            with total_sent:
                total_sentiment = round(win.get("total_sentiment"), 2)
                window_sentiment = round(win.get("window_sentiment"), 2)
                st.metric(value=total_sentiment, delta=window_sentiment, label="TOTAL SENTIMENT")

            fig = px.line(df, x="window_start", y="total_sentiment", title=f"Total sentiment for {event_label}", markers=False)
            st.plotly_chart(fig, use_container_width=True)