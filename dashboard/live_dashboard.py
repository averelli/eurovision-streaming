import streamlit as st
import plotly.express as px
import pandas as pd
from processors.dashboard_consumer import consume_windows

WINDOW_COLUMNS = ["id", "window_start", "window_posts", "window_end", "total_posts", "window_sentiment", "total_sentiment", "window_avg_sentiment", "total_avg_sentiment", "window_platform_counter", "total_platform_counter", "window_top_song", "window_top_country", "window_top_contestant", "total_top_song", "total_top_country", "total_top_contestant", "event_label"]

st.set_page_config(layout="wide")
placeholder = st.empty()
total_df = pd.DataFrame(columns=WINDOW_COLUMNS)
posts_volume_df = pd.DataFrame(columns=["timestamp", "platform", "platform_posts"])

for win in consume_windows(pause_time=3):
    event_label = win["event_label"]
    win_df = pd.DataFrame([win], columns=WINDOW_COLUMNS).replace("none", None)
    total_df = pd.concat([total_df, win_df], ignore_index=True)

    with placeholder:
        with st.container() as dashboard:
            # left column for the stats, right column for the chart
            stats_col, chart_col = st.columns(2)

            with stats_col:
                # Page title
                st.header(f"LIVE DASHBOARD FOR {win["event_label"].upper()}", divider=True)
            
                with st.container(border=True) as stats_table:
                    # Headers
                    top_labels = ["**TOP**", "**SONG**", "**COUNTRY**", "**CONTESTANT**"]
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

                # total sentiment stats
                with st.container(border=True) as sentiment_stats:
                    cols = st.columns(3)
                    with cols[0]:
                        window_sentiment = round(win.get("window_sentiment"), 2)
                        total_sentiment = round(win.get("total_sentiment"), 2)
                        st.metric(value=total_sentiment, delta=window_sentiment, label="TOTAL SENTIMENT")
                    with cols[1]:
                        st.metric(label="AVG SENTIMENT", value=win.get("total_avg_sentiment", 0))
                    with cols[2]:
                        st.metric(label="AVG WINDOW SENTIMENT", value=win.get("window_avg_sentiment", 0))
                        
                # Posts number row
                with st.container(border=True):
                    total_platform_counter = win["total_platform_counter"]
                    window_platform_counter = win["window_platform_counter"]
                    cols = st.columns(3)
                    with cols[0]:
                        st.metric(label="TOTAL POSTS", value=win["total_posts"], delta=win["window_posts"])
                    with cols[1]:
                        st.metric(label="BLUESKY POSTS", value=total_platform_counter.get("Bluesky", 0), delta=window_platform_counter.get("Bluesky", 0))
                    with cols[2]:
                        st.metric(label="TUMBLR POSTS", value=total_platform_counter.get("Tumblr", 0), delta=window_platform_counter.get("Tumblr", 0))

            with chart_col:
                # sentiment over time line chart 
                with st.container(border=True) as line_chart:
                    fig = px.line(total_df, x="window_start", y="total_sentiment", title=f"Total sentiment for {event_label}", markers=False)
                    fig.update_layout(margin=dict(t=25, b=10, l=20, r=20), height=400)
                    st.plotly_chart(fig, use_container_width=True)
                
                # posts volume over time stacked bar chart
                with st.container(border=True) as bar_chart:
                    tumblr_row = [win["window_start"], "Tumblr", win["window_platform_counter"].get("Tumblr", 0)]
                    bluesky_row = [win["window_start"], "Bluesky",  win["window_platform_counter"].get("Bluesky", 0)]
                    posts_volume_df.loc[len(posts_volume_df)] = tumblr_row
                    posts_volume_df.loc[len(posts_volume_df)] = bluesky_row

                    fig = px.bar(posts_volume_df, x="timestamp", y="platform_posts", color="platform", barmode="stack", title="Posts volume by platform")
                    fig.update_layout(height=230, margin=dict(t=25, b=10, l=20, r=20))
                    st.plotly_chart(fig, use_container_width=True)
