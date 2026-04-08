def company_data():
    with open("instantly\\tecblic_data.md", "r") as file:
        data=file.read()
    return data

def sender_info():
    with open("instantly\\sender_info.md", "r") as file:
        data=file.read()
    return data

def email_prompt(df,context):
    email_body_prompt=f"""
    You have to analyze the following data about a person and their organization, and then generate a personalized email outreach message body based on that information.

    {df.to_dict()}
    
    + 
    
    Context : {context}

    +
    
    Sender's Company Information : {company_data()}
    
    +
    
    sender's Information : {sender_info()}
    
        
    ##NOTE :
    
    You have to make the entire email short and broken into 3 parts 
    1 INTRODUCTION : A brief introduction about yourself and your company. (1-2 sentences)
    
    2. VALUE PROPOSITION : A concise statement that highlights the value your product or service can provide to the recipient, based on their role, company, and industry. (2-3 sentences Not Too much) (Provided Context)
    
    3. CALL TO ACTION : A clear and compelling call to action that encourages the recipient to take the next step, such as scheduling a meeting, signing up for a free trial, or visiting your website. (1-2 sentences)
    
    
    The email should be concise, engaging, and relevant to the person's role and their organization's industry. Use the provided data to find common ground, identify potential pain points, or highlight opportunities for collaboration. The tone should be professional yet approachable, aiming to spark interest and encourage a response.
    """
    return email_body_prompt


def followup_1_prompt(df, context):
    followup_1_prompt=f"""
    You have to analyze the following data about a person and their organization, and generate a follow-up email that references the initial outreach but takes a DIFFERENT ANGLE.

    {df.to_dict()}
    
    + 
    
    Context : {context}

    +
    
    Sender's Company Information : {company_data()}
    
    +
    
    sender's Information : {sender_info()}
    
    
    ##NOTE :
    
    This is a FIRST FOLLOW-UP email. Make it short and broken into 3 parts:
    
    1. SOFT REFERENCE : Politely reference your previous email without sounding pushy (1 sentence)
    
    2. NEW ANGLE : Introduce a slightly different angle or benefit of your product/service that complements the initial value proposition. Focus on a pain point or opportunity not heavily emphasized before. (2-3 sentences Only)
    
    3. CALL TO ACTION : A friendly, low-pressure CTA that makes it easy for them to engage (ask for their thoughts, share resources, quick call, etc). (1-2 sentences)
    
    
    Tone: Warm, conversational, and patient. Show that you understand their situation and are genuinely interested in helping, not just selling. Keep it brief so they don't feel overwhelmed.
    """
    return followup_1_prompt


def followup_2_prompt(df, context):
    """Second Follow-up (5-7 days after 1st) - Add new value/social proof"""
    followup_2_prompt=f"""
    You have to analyze the following data about a person and their organization, and generate a SECOND follow-up email that introduces NEW VALUE or proof points.

    {df.to_dict()}
    
    + 
    
    Context : {context}

    +
    
    Sender's Company Information : {company_data()}
    
    +
    
    sender's Information : {sender_info()}
    
    
    ##NOTE :
    
    This is a SECOND FOLLOW-UP email. Make it short and broken into 3 parts:
    
    1. BRIEF CHECK-IN : Acknowledge that they've been busy and express that you wanted to share something valuable (1 sentence)
    
    2. NEW VALUE : Share a specific insight, case study, recent success story, or unique benefit they may not know about. This should be genuinely useful info, not just another pitch. (2-3 sentences)
    
    3. LOW-FRICTION CTA : Offer something tangible: a quick resource, a brief conversation, or a no-commitment option (1-2 sentences)
    
    
    Tone: Helpful expert providing value. Position yourself as a resource, not an aggressive salesperson. Show you've done your homework on their industry/company.
    """
    return followup_2_prompt


def followup_3_prompt(df, context):
    """Third Follow-up (7-10 days after 2nd) - Create urgency/scarcity"""
    followup_3_prompt=f"""
    You have to analyze the following data about a person and their organization, and generate a THIRD follow-up email that introduces light urgency while remaining respectful.

    {df.to_dict()}
    
    + 
    
    Context : {context}

    +
    
    Sender's Company Information : {company_data()}
    
    +
    
    sender's Information : {sender_info()}
    
    
    ##NOTE :
    
    This is a THIRD FOLLOW-UP email. Make it short and broken into 3 parts:
    
    1. ACKNOWLEDGMENT : Briefly acknowledge that you've reached out a couple of times and respect their time (1 sentence)
    
    2. RELEVANT URGENCY : Add context about why now is a good time to engage: limited availability, seasonal opportunity, recent industry trend affecting their company, or time-sensitive benefit. Make it authentic and relevant to their situation, NOT generic urgency. (2-3 sentences)
    
    3. FINAL CTA : Offer a specific, easy next step with a loose timeline (e.g., "let me know if the next 2 weeks work for a quick sync") (1-2 sentences)
    
    
    Tone: Professional and respectful. Show understanding that they're busy, but also express genuine confidence that a conversation would be valuable. Avoid desperation.
    """
    return followup_3_prompt


def followup_4_prompt(df, context):
    """Fourth Follow-up (Final attempt) - Curiosity & graceful exit"""
    followup_4_prompt=f"""
    You have to analyze the following data about a person and their organization, and generate a FINAL follow-up email that either opens a last door OR gracefully steps back.

    {df.to_dict()}
    
    + 
    
    Context : {context}

    +
    
    Sender's Company Information : {company_data()}
    
    +
    
    sender's Information : {sender_info()}
    
    
    ##NOTE :
    
    This is a FOURTH & FINAL FOLLOW-UP email. Make it short and broken into 3 parts:
    
    1. RESPECTFUL HONESTY : Acknowledge that you've reached out multiple times and you understand they may not be interested right now (1-2 sentences)
    
    2. CURIOSITY OR VALUE : Either ask a genuine question about their current priorities (curiosity angle), or offer one last piece of valuable insight or resource without expecting a response (1-2 sentences)
    
    3. GRACEFUL EXIT : Leave the door open without being pushy - let them know they can reach out anytime, or that you'll check in again in 6 months, or simply wish them well. (1-2 sentences)
    
    
    Tone: Mature, respectful, and professional. Show that you value the relatiognship over the immediate sale. Make them feel they can still reach out guilt-free, or come back when the time is right.
    """
    return followup_4_prompt


