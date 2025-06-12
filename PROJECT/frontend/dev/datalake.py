from streamlit_file_browser import st_file_browser
from dev.minio import get_minio_client
import streamlit as st
from urllib.parse import quote
import mimetypes
import os
import math

# --------------- HELPERS --------------- #

def human_readable_size(size_bytes):
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = int(math.floor(math.log(size_bytes, 1024)))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"

def file_icon(filename: str):
    ext = filename.lower().split('.')[-1]
    icons = {
        "csv": "🧾", "parquet": "🪵", "json": "🧬",
        "txt": "📜", "xml": "📄", "delta": "🌀",
        "mp3": "🎵", "mp4": "🎞️", "wav": "🔊",
        "jpg": "🖼️", "jpeg": "🖼️", "png": "🖼️",
        "pdf": "📕", "zip": "🗜️"
    }
    return icons.get(ext, "📄")

def list_minio_objects(bucket: str, prefix="", recursive=True):
    """List all objects optionally recursively."""
    client = get_minio_client()
    return list(client.list_objects(bucket, prefix=prefix, recursive=recursive))

def list_minio_folders_and_files(bucket: str, prefix=""):
    """List folders and files under a prefix (non-recursive)."""
    minio = get_minio_client()
    folders = set()
    files = []

    for obj in minio.list_objects(bucket, prefix=prefix, recursive=False):
        suffix = obj.object_name[len(prefix):]
        parts = suffix.split("/")
        if len(parts) > 1:
            folders.add(parts[0])
        else:
            files.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified,
                "type": mimetypes.guess_type(obj.object_name)[0] or "unknown"
            })

    return sorted(folders), sorted(files, key=lambda x: x["name"])

# --------------- UI COMPONENT --------------- #
def show_file_browser_minio(bucket: str = None):
    # Setup MinIO client
    minio = get_minio_client()
    all_buckets = [b.name for b in minio.list_buckets()]
    selected_bucket = bucket if bucket in all_buckets else st.selectbox("📦 Choose a Bucket", all_buckets)

    # Initialize session state
    if "current_prefix" not in st.session_state:
        st.session_state.current_prefix = ""
    if "view_mode_toggle" not in st.session_state:
        st.session_state.view_mode_toggle = False  # False = Folders, True = Flat

    # Toggle between folder and flat mode
    st.toggle("🧾 Show Flat File List", key="view_mode_toggle")

    # Set view mode
    view_mode = "Flat" if st.session_state.view_mode_toggle else "Folders"

    # Breadcrumb Navigation (Horizontal)
    prefix_parts = st.session_state.current_prefix.strip("/").split("/") if st.session_state.current_prefix else []
    breadcrumb_paths = []
    accumulated = ""

    for part in prefix_parts:
        accumulated += f"{part}/"
        breadcrumb_paths.append((part, accumulated))

    breadcrumb_paths = [("🏠", "")] + breadcrumb_paths

    if breadcrumb_paths:
        cols = st.columns(len(breadcrumb_paths))
        for col, (label, path) in zip(cols, breadcrumb_paths):
            with col:
                if st.button(label, key=f"crumb_{path}"):
                    st.session_state.current_prefix = path
                    st.rerun()

    # Show current path
    st.caption(f"📍 Path: `/{st.session_state.current_prefix}`" if st.session_state.current_prefix else "📍 Path: `/`")

    # Flat View
    if view_mode == "Flat":
        st.success("📄 Showing all files recursively.")
        files = list_minio_objects(selected_bucket, prefix=st.session_state.current_prefix, recursive=True)
        for file in files:
            icon = file_icon(file.object_name)
            st.markdown(f"{icon} `{file.object_name}`")

    # Folder View
    else:
        st.success("📁 Folder-style navigation")

        # Go Up button
        if st.session_state.current_prefix:
            parent = "/".join(st.session_state.current_prefix.rstrip("/").split("/")[:-1])
            if st.button("⬅️ Go Up"):
                st.session_state.current_prefix = parent + "/" if parent else ""
                st.rerun()

        # List folders & files
        folders, files = list_minio_folders_and_files(selected_bucket, st.session_state.current_prefix)

        if folders:
            st.markdown("### 📂 Folders")
            for folder in folders:
                if st.button(f"📁 {folder}/"):
                    st.session_state.current_prefix += f"{folder}/"
                    st.rerun()

        if files:
            st.markdown("### 📄 Files")
            cols = st.columns(3)
            for i, file in enumerate(files):
                col = cols[i % 3]
                with col:
                    with st.container(border=True):
                        icon = file_icon(file["name"])
                        name = os.path.basename(file["name"])
                        st.markdown(f"**{icon} `{name}`**")
                        st.caption(f"🕒 {file['last_modified'].strftime('%Y-%m-%d %H:%M')}")
                        st.caption(f"📦 {human_readable_size(file['size'])}")



# ----------- DATA LAKE BROWSER -----------#
def show_file_browser_local(path: str):
    """Local development mode file explorer."""
    return st_file_browser(
        path,
        key="local_browser",
        show_choose_file=True,
        show_choose_folder=True,
        show_download_file=True,
        show_upload_file=True,
    )
    
def show_datalake_explorer(env: str = "production", bucket: str = None):
    """Switch between dev (local) and prod (MinIO) modes, with optional bucket."""
    if env == "development":
        show_file_browser_local("./test_data_lake")
    else:
        show_file_browser_minio(bucket=bucket)