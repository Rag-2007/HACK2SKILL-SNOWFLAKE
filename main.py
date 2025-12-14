import streamlit as st
import cloudinary
import cloudinary.uploader
import db 

cloudinary.config(
    cloud_name=st.secrets["cloudinary"]["cloud_name"],
    api_key=st.secrets["cloudinary"]["api_key"],
    api_secret=st.secrets["cloudinary"]["api_secret"]
)

def upload_image(uploaded_file, property_id, room):
    result = cloudinary.uploader.upload(
        uploaded_file,
        folder=f"inspections/{property_id}/{room}"
    )
    return result["secure_url"]

res = st.sidebar.selectbox(
    "SELECT YOUR ROLE",
    ("INSPECTOR", "USER")
)

if res == "INSPECTOR":
        tab1 , tab2 = st.tabs(["INSPECTION","RISK-EVALUVATION"])
        with tab1 :
            with st.form("inspect-form") :
                st.header("INSPECTION FINDINGS FORM")
                pid = st.text_input("Enter Property ID")
                rname = st.text_input("Enter Room Name")
                uploaded_file = st.file_uploader("Upload the Defect Region (PHOTO / VIDEO)",type=['jpg','png','mp4'])
                notes = st.text_input("Enter the Inspection Note")
                isDone = st.form_submit_button("UPLOAD") 
                
            if isDone and pid and rname and notes and uploaded_file:
                url = upload_image(
                    uploaded_file=uploaded_file,
                    property_id=pid,
                    room=rname
                )
                db.insert_image(url, notes)
                db.insert_inspection(pid, rname)
                st.success("INSPECTION RECORDED SUCESSFULLY")  
            
            elif isDone:
                st.warning("ALL FIELDS ARE MANDATORY")
    
        with tab2:
            st.header("AI Risk Evaluation")
            lst = db.select_pids() 
            pid = st.selectbox("Select the Property", lst)
            isDone  = st.button("GENERATE AI REPORT")
            if isDone and pid:
                db.ai_summary_generate(pid) 
                st.success("AI inspection report generated")
            elif isDone :
                st.warning("ALL FIELDS ARE MANDATORY")

elif res == "USER":
    st.header("🏠 Property Inspection Report")
    pids = db.select_pids_risk()
    if not pids:
        st.info("No AI inspection reports available yet.")
    else:
        pid = st.selectbox("Select Property", pids)
        risk, summary = db.get_risk_summary(pid)
        images = db.get_images(pid)

        st.subheader(f"Property ID: {pid}")

        if risk < 30:
            st.success(f"🟢 Low Risk ({risk})")
        elif risk < 60:
            st.warning(f"🟠 Moderate Risk ({risk})")
        else:
            st.error(f"🔴 High Risk ({risk})")

        st.markdown("### 🧠 AI Inspection Summary")
        st.write(summary)

        st.markdown("### 📷 Inspection Images")
        st.image(images, width=150)





