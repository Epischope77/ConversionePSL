import streamlit as st
from streamlit_option_menu import option_menu

def searchable_selectbox(label, options, key=None):
    # Usa option_menu per mostrare un menu a tendina custom con ricerca
    selected = option_menu(
        menu_title=label,
        options=options,
        icons=[""] * len(options),
        menu_icon="cast",
        default_index=0,
        orientation="vertical",
        key=key
    )
    return selected
