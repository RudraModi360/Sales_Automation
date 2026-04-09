def email_format():
    ##main email format
    main_email_format="""
    {{main_email_introduction}}
    <br><br>
    {{main_email_value_proposition}}
    <br><br>
    {{main_email_call_to_action}}
    <br><br><br>
    --<br>
    Warm Regards,<br>
    Dhaval Patel | Sales Catalyst<br>
    Buisness Team | Tecblic Pvt Ltd, India<br>
    M: +91 9998714535 | +91 7984017943
    """
    
    ##follow-up email formats
    followup_1_email_format="""
    {{followup_1_introduction}}
    <br><br>
    {{followup_1_value_proposition}}
    <br><br>
    {{followup_1_call_to_action}}
    <br><br><br>
    --
    Warm Regards,<br>
    Dhaval Patel | Sales Catalyst<br>
    Buisness Team | Tecblic Pvt Ltd, India<br>
    M: +91 9998714535 | +91 7984017943
    """
    
    ## follow-up email formats
    followup_2_email_format="""
    {{followup_2_introduction}}
    <br><br>
    {{followup_2_value_proposition}}
    <br><br>
    {{followup_2_call_to_action}}
    <br><br><br>
    --<br>
    Warm Regards,<br>
    Dhaval Patel | Sales Catalyst<br>
    Buisness Team | Tecblic Pvt Ltd, India<br>
    M: +91 9998714535 | +91 7984017943
    """
    
    ## follow-up email formats
    followup_3_email_format="""
    {{followup_3_introduction}}
    <br><br>
    {{followup_3_value_proposition}}
    <br><br>
    {{followup_3_call_to_action}}
    <br><br><br>
    --<br>
    Warm Regards,<br>
    Dhaval Patel | Sales Catalyst<br>
    Buisness Team | Tecblic Pvt Ltd, India<br>
    M: +91 9998714535 | +91 7984017943
    """
    
    ## follow-up email formats
    followup_4_email_format="""
    {{followup_4_introduction}}
    <br><br>
    {{followup_4_value_proposition}}
    <br><br>
    {{followup_4_call_to_action}}
    <br><br><br>
    --<br>
    Warm Regards,<br>   
    Dhaval Patel | Sales Catalyst<br>
    Buisness Team | Tecblic Pvt Ltd, India<br>
    M: +91 9998714535 | +91 7984017943
    """
    return [main_email_format,followup_1_email_format,followup_2_email_format,followup_3_email_format,followup_4_email_format]

def get_subject_line():
    import random
    fixed_subject_line=[
 "Power Your Growth with Tecblic IT Expertise",
 "Your Trusted Partner for Scalable IT Solutions",
 "Accelerate Innovation with Tecblic Tech Talent",
 "One Stop for IT Services and Staff Augmentation",
 "Build Smarter Scale Faster with Tecblic Experts",
 "Elevate Your Business with Tecblic IT Services",
 "Tech Talent and IT Solutions All in One",
 "Scale Confidently with Tecblic IT and Talent",
 "Transform Ideas Faster with Tecblic IT Expertise",
 "Reliable IT Services and Talent by Tecblic"
    ]
    idx = random.randint(0, 9)
    return fixed_subject_line[idx]
    
