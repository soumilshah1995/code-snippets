import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import tempfile

# Set page config
st.set_page_config(
    page_title="Iceberg Service Insights",
    page_icon="❄️",
    layout="wide"
)

# App title and description
st.title("❄️ Iceberg Service Insights Dashboard")
st.markdown("""
This dashboard provides comprehensive insights into your Iceberg tables' performance, data evolution, and service metrics.
""")

# Initialize DuckDB with required extensions
@st.cache_resource
def initialize_duckdb():
    conn = duckdb.connect(database=':memory:')

    # Install and load required extensions
    conn.execute("INSTALL aws;")
    conn.execute("LOAD aws;")
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute("INSTALL iceberg;")
    conn.execute("LOAD iceberg;")
    conn.execute("INSTALL parquet;")
    conn.execute("LOAD parquet;")

    # Load AWS credentials
    conn.execute("CALL load_aws_credentials();")

    return conn

# Get DuckDB connection
conn = initialize_duckdb()

# Sidebar for input options
st.sidebar.header("Iceberg Table Source")
input_option = st.sidebar.radio("Select input method:", ["Upload Metadata File", "S3 Path", "Local Path"])

metadata_path = None

if input_option == "Upload Metadata File":
    uploaded_file = st.sidebar.file_uploader("Choose an Iceberg metadata JSON file", type=["json"])
    if uploaded_file is not None:
        # Save uploaded file to a temporary file
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as temp_file:
            temp_file.write(uploaded_file.getvalue())
            metadata_path = temp_file.name
        st.sidebar.success(f"File uploaded successfully!")
elif input_option == "S3 Path":
    default_s3_path = "s3://soumil-dev-bucket-1995/warehouse/sales/metadata/00001-cae4181c-e8ec-4d6a-97b6-c4732cc9c434.metadata.json"
    metadata_path = st.sidebar.text_input("Enter S3 path to metadata file:", value=default_s3_path)
else:
    default_local_path = "data/iceberg/lineitem_iceberg"
    metadata_path = st.sidebar.text_input("Enter local path to Iceberg table:", value=default_local_path)

# Add button to analyze
analyze_button = st.sidebar.button("Analyze Table")

# Helper functions for insights
def calculate_data_growth(snapshots_df, manifest_df):
    """Calculate data growth between snapshots"""
    if 'timestamp' not in snapshots_df.columns or len(snapshots_df) <= 1:
        return pd.DataFrame()

    growth_data = []
    sorted_snapshots = snapshots_df.sort_values('timestamp')

    for i in range(1, len(sorted_snapshots)):
        prev_snapshot = sorted_snapshots.iloc[i-1]
        curr_snapshot = sorted_snapshots.iloc[i]

        # Calculate time between snapshots
        time_diff = (curr_snapshot['timestamp'] - prev_snapshot['timestamp']).total_seconds() / 3600  # hours

        # Get records for current snapshot
        current_records = manifest_df[manifest_df['manifest_sequence_number'] == curr_snapshot['sequence_number']]['record_count'].sum()
        previous_records = manifest_df[manifest_df['manifest_sequence_number'] == prev_snapshot['sequence_number']]['record_count'].sum()

        # Calculate growth
        record_growth = current_records - previous_records
        growth_percent = (record_growth / previous_records * 100) if previous_records > 0 else 0

        growth_data.append({
            'previous_snapshot': prev_snapshot['snapshot_id'],
            'current_snapshot': curr_snapshot['snapshot_id'],
            'timestamp': curr_snapshot['timestamp'],
            'time_diff_hours': time_diff,
            'record_growth': record_growth,
            'growth_percent': growth_percent,
            'previous_records': previous_records,
            'current_records': current_records
        })

    return pd.DataFrame(growth_data)

def calculate_file_stats(manifest_df):
    """Calculate statistics about data files"""
    if manifest_df.empty:
        return {}

    stats = {
        'total_files': len(manifest_df),
        'avg_records_per_file': manifest_df['record_count'].mean(),
        'max_records_in_file': manifest_df['record_count'].max(),
        'min_records_in_file': manifest_df['record_count'].min(),
        'std_dev_records': manifest_df['record_count'].std(),
        'total_records': manifest_df['record_count'].sum()
    }

    # File format distribution
    if 'file_format' in manifest_df.columns:
        stats['format_distribution'] = manifest_df['file_format'].value_counts().to_dict()

    # Content type distribution
    if 'content' in manifest_df.columns:
        stats['content_distribution'] = manifest_df['content'].value_counts().to_dict()

    # Status distribution
    if 'status' in manifest_df.columns:
        stats['status_distribution'] = manifest_df['status'].value_counts().to_dict()

    return stats

def extract_operation_metrics(snapshots_df, manifest_df):
    """Extract metrics about operations (add/delete)"""
    if manifest_df.empty or snapshots_df.empty:
        return pd.DataFrame()

    operations = []

    for idx, snapshot in snapshots_df.iterrows():
        seq_num = snapshot['sequence_number']
        snapshot_manifests = manifest_df[manifest_df['manifest_sequence_number'] == seq_num]

        if 'status' in snapshot_manifests.columns:
            adds = snapshot_manifests[snapshot_manifests['status'] == 'ADDED']['record_count'].sum()
            deletes = snapshot_manifests[snapshot_manifests['status'] == 'DELETED']['record_count'].sum()

            operations.append({
                'snapshot_id': snapshot['snapshot_id'],
                'timestamp': snapshot['timestamp'] if 'timestamp' in snapshot else pd.NaT,
                'sequence_number': seq_num,
                'added_records': adds,
                'deleted_records': deletes,
                'net_change': adds - deletes,
                'manifest_count': len(snapshot_manifests)
            })

    return pd.DataFrame(operations)

# Main content
if metadata_path and analyze_button:
    try:
        # Create tabs for different analyses
        tab1, tab2, tab3, tab4 = st.tabs(["Overview", "Data Evolution", "Storage Insights", "Operation Analysis"])

        # Get snapshot data
        try:
            snapshots_df = conn.execute(f"""
                SELECT * FROM iceberg_snapshots('{metadata_path}')
            """).fetchdf()

            # Convert timestamp to datetime if it's not already
            if not snapshots_df.empty and 'timestamp_ms' in snapshots_df.columns:
                if snapshots_df['timestamp_ms'].dtype == 'object':
                    snapshots_df['timestamp'] = pd.to_datetime(snapshots_df['timestamp_ms'])
                else:
                    snapshots_df['timestamp'] = pd.to_datetime(snapshots_df['timestamp_ms'])
        except Exception as e:
            st.error(f"Error fetching snapshots: {str(e)}")
            snapshots_df = pd.DataFrame()

        # Get manifest data
        try:
            manifest_df = conn.execute(f"""
                SELECT * FROM iceberg_metadata('{metadata_path}')
            """).fetchdf()
        except Exception as e:
            st.error(f"Error fetching metadata: {str(e)}")
            manifest_df = pd.DataFrame()

        # Get schema information
        try:
            schema_df = conn.execute(f"""
                SELECT * FROM iceberg_schema('{metadata_path}')
            """).fetchdf()
        except Exception as e:
            schema_df = pd.DataFrame()

        # Get table properties
        try:
            properties_df = conn.execute(f"""
                SELECT * FROM iceberg_table_properties('{metadata_path}')
            """).fetchdf()
        except Exception as e:
            properties_df = pd.DataFrame()

        # Calculate insights
        if not snapshots_df.empty and not manifest_df.empty:
            growth_df = calculate_data_growth(snapshots_df, manifest_df)
            file_stats = calculate_file_stats(manifest_df)
            operations_df = extract_operation_metrics(snapshots_df, manifest_df)

        # Tab 1: Overview
        with tab1:
            st.header("Iceberg Table Overview")

            # Top metrics
            metric_cols = st.columns(4)

            with metric_cols[0]:
                st.metric("Total Snapshots", len(snapshots_df) if not snapshots_df.empty else 0)

            with metric_cols[1]:
                st.metric("Total Files", len(manifest_df) if not manifest_df.empty else 0)

            with metric_cols[2]:
                if not manifest_df.empty and 'record_count' in manifest_df.columns:
                    st.metric("Total Records", f"{manifest_df['record_count'].sum():,}")
                else:
                    st.metric("Total Records", "N/A")

            with metric_cols[3]:
                if not snapshots_df.empty and 'timestamp' in snapshots_df.columns:
                    latest_update = snapshots_df['timestamp'].max()
                    days_ago = (datetime.now() - latest_update).days
                    st.metric("Last Updated", f"{days_ago} days ago")
                else:
                    st.metric("Last Updated", "N/A")

            # Table schema
            if not schema_df.empty:
                st.subheader("Table Schema")
                st.dataframe(schema_df, use_container_width=True)

            # Table properties
            if not properties_df.empty:
                st.subheader("Table Properties")
                st.dataframe(properties_df, use_container_width=True)

            # Recent snapshots
            if not snapshots_df.empty:
                st.subheader("Recent Snapshots")
                if 'timestamp' in snapshots_df.columns:
                    recent_snapshots = snapshots_df.sort_values('timestamp', ascending=False).head(5)
                    st.dataframe(recent_snapshots, use_container_width=True)
                else:
                    st.dataframe(snapshots_df.head(5), use_container_width=True)

        # Tab 2: Data Evolution
        with tab2:
            st.header("Data Evolution")

            if not snapshots_df.empty and 'timestamp' in snapshots_df.columns:
                # Timeline of snapshots
                st.subheader("Snapshots Timeline")

                if len(snapshots_df) > 1:
                    fig = px.line(
                        snapshots_df.sort_values('timestamp'),
                        x='timestamp',
                        y='sequence_number',
                        markers=True,
                        title="Snapshots Over Time"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Data growth over time
                if not growth_df.empty:
                    st.subheader("Data Growth Analysis")

                    growth_cols = st.columns(2)

                    with growth_cols[0]:
                        fig = px.bar(
                            growth_df,
                            x='timestamp',
                            y='record_growth',
                            title="Record Growth Between Snapshots"
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    with growth_cols[1]:
                        fig = px.line(
                            growth_df,
                            x='timestamp',
                            y='current_records',
                            markers=True,
                            title="Total Records Over Time"
                        )
                        st.plotly_chart(fig, use_container_width=True)

                    # Growth rate
                    st.subheader("Growth Rate Analysis")
                    if len(growth_df) > 1:
                        fig = px.scatter(
                            growth_df,
                            x='time_diff_hours',
                            y='growth_percent',
                            size='record_growth',
                            hover_data=['timestamp', 'current_snapshot'],
                            title="Growth Rate vs Time Between Snapshots"
                        )
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Insufficient data to analyze evolution. Need multiple snapshots with timestamps.")

        # Tab 3: Storage Insights
        with tab3:
            st.header("Storage Insights")

            if not manifest_df.empty:
                # File distribution
                st.subheader("File Size Distribution")

                if 'record_count' in manifest_df.columns:
                    fig = px.histogram(
                        manifest_df,
                        x='record_count',
                        nbins=20,
                        title="Distribution of Records per File"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # File stats
                if file_stats:
                    st.subheader("File Statistics")

                    stats_cols = st.columns(3)

                    with stats_cols[0]:
                        st.metric("Avg Records/File", f"{file_stats['avg_records_per_file']:.1f}")
                        st.metric("Min Records/File", file_stats['min_records_in_file'])

                    with stats_cols[1]:
                        st.metric("Max Records/File", file_stats['max_records_in_file'])
                        st.metric("StdDev Records", f"{file_stats['std_dev_records']:.1f}")

                    with stats_cols[2]:
                        if 'format_distribution' in file_stats:
                            st.write("File Formats:")
                            for fmt, count in file_stats['format_distribution'].items():
                                st.write(f"- {fmt}: {count}")

                # Content and status distribution
                dist_cols = st.columns(2)

                with dist_cols[0]:
                    if 'manifest_content' in manifest_df.columns:
                        content_counts = manifest_df['manifest_content'].value_counts().reset_index()
                        content_counts.columns = ['Content Type', 'Count']

                        st.subheader("Content Type Distribution")
                        fig = px.pie(
                            content_counts,
                            values='Count',
                            names='Content Type',
                            title="Manifest Content Types"
                        )
                        st.plotly_chart(fig)

                with dist_cols[1]:
                    if 'status' in manifest_df.columns:
                        status_counts = manifest_df['status'].value_counts().reset_index()
                        status_counts.columns = ['Status', 'Count']

                        st.subheader("Status Distribution")
                        fig = px.pie(
                            status_counts,
                            values='Count',
                            names='Status',
                            title="File Status Distribution"
                        )
                        st.plotly_chart(fig)
            else:
                st.info("No manifest data available for storage insights.")

        # Tab 4: Operation Analysis
        with tab4:
            st.header("Operation Analysis")

            if not operations_df.empty:
                # Operation metrics
                st.subheader("Add/Delete Operations")

                op_cols = st.columns(2)

                with op_cols[0]:
                    if 'timestamp' in operations_df.columns:
                        fig = px.bar(
                            operations_df.sort_values('timestamp'),
                            x='timestamp',
                            y=['added_records', 'deleted_records'],
                            barmode='group',
                            title="Records Added vs Deleted by Snapshot"
                        )
                        st.plotly_chart(fig, use_container_width=True)

                with op_cols[1]:
                    if 'timestamp' in operations_df.columns:
                        fig = px.line(
                            operations_df.sort_values('timestamp'),
                            x='timestamp',
                            y='net_change',
                            title="Net Record Change by Snapshot"
                        )
                        st.plotly_chart(fig, use_container_width=True)

                # Manifest file count by snapshot
                st.subheader("Manifest Files per Snapshot")

                if 'timestamp' in operations_df.columns:
                    fig = px.bar(
                        operations_df.sort_values('timestamp'),
                        x='timestamp',
                        y='manifest_count',
                        title="Manifest Files per Snapshot"
                    )
                    st.plotly_chart(fig, use_container_width=True)

                # Operation data table
                st.subheader("Operation Details")
                st.dataframe(operations_df, use_container_width=True)
            else:
                st.info("Insufficient data to analyze operations. Need snapshot and manifest data with status information.")

    except Exception as e:
        st.error(f"Error analyzing Iceberg table: {str(e)}")
        st.error("Please check if the metadata path is correct and accessible.")
else:
    st.info("Please provide an Iceberg table path and click 'Analyze Table' to begin analysis.")

# Clean up temporary file if created
if input_option == "Upload Metadata File" and 'temp_file' in locals():
    try:
        os.unlink(temp_file.name)
    except:
        pass
