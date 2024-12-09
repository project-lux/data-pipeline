from pipeline.process.base.mapper import Mapper
from pipeline.process.utils.mapper_utils import make_datetime
from cromulent import model, vocab
import requests
import json


class ArticMapper(Mapper):
    def __init__(self, config):
        Mapper.__init__(self, config)
        self.handle_group = self.handle_person
        
    def guess_type(self, rec):
        #this is not ideal because some groups are is_artist = True (ex agents/44312)
        is_artist = rec.get('is_artist')
        api_model = rec.get('api_model','')
        if api_model == 'artworks':
            topcls = model.HumanMadeObject
        elif api_model == 'exhibitions':
            topcls = model.Activity
        elif api_model == 'agents' and is_artist:
            topcls = model.Person
        else:
            topcls = model.Group
        return topcls

    def do_setup(self, rec, rectype=""):
        guess = self.guess_type(rec)
        if not guess:
           return None
        if not rectype:
           topcls = guess
        else:
           topcls = getattr(model, rectype)

        if topcls in [model.Person,model.Group]:
            URI = "https://api.artic.edu/api/v1/agents/"
        elif topcls == model.HumanMadeObject:
            URI = "https://api.artic.edu/api/v1/artworks/"
        elif topcls == model.Activity:
            URI = "https://api.artic.edu/api/v1/exhibitions/"

        top = topcls(ident=f"{URI}{str(rec['id'])}")
        return(top, topcls)

    def transform(self, record, rectype=""):
        (top, topcls) = self.do_setup(record, rectype)    
        self.handle_common(record, top)

        if not rectype:
            rectype = topcls.__name__
        #handle classes
        fn = getattr(self, f"handle_{rectype.lower()}")
        if fn:
            fn(record, top, topcls)
        
        #return {'identifier': rec['id'], 'data': data, 'source': 'artic'}
        return top
    def handle_common(self, rec, top):
        pref = rec.get('title','')
        if pref:
            top.identified_by  = vocab.PrimaryName(content=pref)
        alt = rec.get('alt_titles',[])
        if alt:
            for a in alt:
                top.identified_by = vocab.AlternateName(content=a)

        desc = rec.get('description','')
        if desc:
            top.referred_to_by = vocab.Description(content=desc)
        short_desc = rec.get('short_description','')
        if short_desc:
            top.referred_to_by = vocab.Description(content=short_desc)

    def handle_timespan(self, event, date1, date2=None, date3=None):
        ts = model.TimeSpan()
        if type(date1) == int:
            date1 = str(date1)
        if len(date1) > 10: 
            date1 = date1.rsplit('T')[0]
        begins = make_datetime(date1)
        if event in [model.Birth, model.Formation, model.Death, model.Dissolution]:
            ts.begin_of_the_begin = begins[0]
            ts.end_of_the_end = begins[1]
            if date3:
                ts.identified_by = vocab.DisplayName(date3)
            else:
                ts.identified_by = vocab.DisplayName(content=f'{str(date1)}')
        else:
            if date2:
                if type(date2) == int:
                    date2 = str(date2)
                if len(date2) > 10:
                    date2 = date2.rsplit('T')[0]
            ends = make_datetime(date2)
            if begins and ends:
                ts.begin_of_the_begin = begins[0]
                ts.end_of_the_begin = begins[1]
                ts.begin_of_the_end = ends[0]
                ts.end_of_the_end = ends[1]
                if date3:
                    ts.identified_by = vocab.DisplayName(date3)
                else:
                    ts.identified_by = vocab.DisplayName(content=f'{str(date1)} - {str(date2)}')
            elif begins:
                ts.begin_of_the_begin = begins[0]
                ts.end_of_the_end = begins[1]
                if date3:
                    ts.identified_by = vocab.DisplayName(date3)
                else:
                    ts.identified_by = vocab.DisplayName(content=f'{str(date1)}')
            elif ends:
                ts.begin_of_the_begin = ends[0]
                ts.end_of_the_end = ends[1]
                if date3:
                    ts.identified_by = vocab.DisplayName(date3)
                else:
                    ts.identified_by = vocab.DisplayName(content=f'{str(date2)}')
        event.timespan = ts

    def handle_documents(self, doc_ids, top):
        #sounds, images, texts
        for doc in doc_ids:
            try:
                resp = requests.get(f'https://api.artic.edu/api/v1/assets/{doc}')
                if resp.status_code == 200:
                    data = resp.json()
                    asset_type = data['data']['type']
                    asset_id = data['data']['id']
                    content = data['data']['content']
                    title = data['data']['title']
                    if asset_type == "image":
                        vi = model.VisualItem()
                        svc = model.DigitalService()
                        img = vocab.DigitalImage(label=title)
                        vi.digitally_shown_by = img
                        img.digitally_available_via = svc
                        svc.access_point = model.DigitalObject(ident=f'https://www.artic.edu/iiif/2/{asset_id}/info.json')
                        top.representation = vi
                    elif asset_type == "sound":
                        sound = vocab.AudioVisualContent()
                        do = model.DigitalObject(label=title)
                        do.classified_as = model.Type(ident='http://vocab.getty.edu/page/aat/300312045',label="digital audio formats")
                        sound.digitally_carried_by = do
                        do.access_point = model.DigitalObject(ident=content)
                        top.subject_of = sound    
                    elif asset_type == "text":
                        text = model.LinguisticObject()
                        do = model.DigitalObject(label=title)
                        text.digitally_carried_by = do 
                        do.access_point = model.DigitalObject(ident=content)
                        top.subject_of = text
                    else:
                        #some unknown type
                        pass
            except:
                return
    def handle_person(self, rec, top, topcls):
        agent_type = rec.get('agent_type_title')
        with open('/Users/kd736/Desktop/artic/data/artic-agent-types.json') as file:
            agent_types = json.loads(file.read())
            try:
                agent_AAT = agent_types[agent_type]
            except:
                agent_AAT = None
        #dates and equivs for both person and group
        dob = rec.get('birth_date')
        pob = rec.get('birth_place')
        dod = rec.get('death_date')
        pod = rec.get('death_place')
        ulan = rec.get('ulan_id','')
        if topcls == model.Person:
            if agent_AAT:
                top.classified_as = model.Type(f'https://vocab.getty.edu/aat/{agent_AAT}',label=agent_type)
            birth = model.Birth()
            if dob:
                self.handle_timespan(birth, dob)
                top.born = birth
            if pob:
                birth.took_place_at = model.Place(ident=pob)
            if ulan:
                top.equivalent = model.Person(ident=f"https://vocab.getty.edu/ulan/{ulan}")
        else:
            if agent_AAT:
                top.classified_as = model.Type(f'https://vocab.getty.edu/aat/{agent_AAT}',label=agent_AAT)  
            formation = model.Formation()
            if dob:
                self.handle_timespan(formation, dob)
                top.formed_by = formation
            if pob:
                formation.took_place_at = model.Place(label=pob)
            if ulan:
                top.equivalent = model.Group(ident=f"https://vocab.getty.edu/ulan/{ulan}")

        if topcls == model.Person:
            death = model.Death()
            if dod:
                self.handle_timespan(death, dod)
                top.died = death
            if pod:
                death.took_place_at = model.Place(ident=pod)         
        else:
            dissolution = model.Dissolution()
            if dod:
                self.handle_timespan(dissolution, dod)
                top.dissolved_by = dissolution
            if pod:
                dissolution.took_place_at = model.Place(label=pod)

    def handle_humanmadeobject(self, rec, top, topcls):
        #production, dates, accno
        prod = model.Production()
        start_date = rec.get('date_start')
        end_date = rec.get('date_end')
        dd = rec.get('date_display','')
        origin_place = rec.get('place_of_origin','')
        if start_date:
            self.handle_timespan(prod, start_date, end_date, dd)
        if origin_place:
            prod.took_place_at = model.Place(label=origin_place)
        top.produced_by = prod
        accNo = rec.get('main_reference_number','')
        top.identified_by = vocab.AccessionNumber(content=accNo)

        #sounds, texts, images
        doc_ids = rec.get('document_ids',[])
        self.handle_documents(doc_ids,top)

        #artists
        artists = {}
        artists_names = rec.get('artist_titles',[])
        artists_ids = rec.get('artist_ids',[])
        artists.update(dict(zip(artists_ids, artists_names)))
        for key, value in artists.items():
            prod.carried_out_by = model.Person(ident=f'https://api.artic.edu/api/v1/agents/{str(key)}',label=value)

        #techniques, mediums, styles, subjects
        with open('/Users/kd736/Desktop/artic/data/artic_category_terms.json','r') as j:
            try:
                categoryterms = json.loads(j.read())
            except:
                categoryterms = None
        tech_titles = rec.get('technique_titles',[])
        tech_ids = rec.get('technique_ids',[])
        techniques = dict(zip(tech_ids, tech_titles))
        for key, value in techniques.items():
            categoryterm = categoryterms[key]
            if categoryterm:
                prod.technique = model.Type(ident=f'https://vocab.getty.edu/aat/{categoryterm}',label=value)
            else:
                prod.technique = model.Type(label=value)
        medium_titles = rec.get('material_titles',[])
        medium_ids = rec.get('material_ids',[])
        materials = dict(zip(medium_ids,medium_titles))
        for key, value in materials.items():
            categoryterm = categoryterms[key]
            if categoryterm:
                top.made_of = vocab.Material(ident=f'https://vocab.getty.edu/aat/{categoryterm}',label=value)
            else:
                top.referred_to_by = vocab.MaterialStatement(content=value)
        vi = model.VisualItem()
        style_titles = rec.get('style_titles',[])
        style_ids = rec.get('style_ids',[])
        styles = dict(zip(style_ids, style_titles))
        for key, value in styles.items():
            categoryterm = categoryterms[key]
            if categoryterm:
                vi.classified_as = vocab.Style(ident=f'https://vocab.getty.edu/aat/{categoryterm}',label=value)
                top.shows = vi
            else:
                vi.classified_as = model.Type(label=value) 
                top.shows = vi
        subject_ids = rec.get('subject_ids',[])
        subject_titles = rec.get('subject_titles',[])
        subjects = dict(zip(subject_ids, subject_titles))
        for key, value in subjects.items():
            categoryterm = categoryterms[key]
            if categoryterm:
                vi.represents = model.Type(ident=f'https://vocab.getty.edu/aat/{categoryterm}',label=value)
                top.shows = vi
            else:
                vi.represents = model.Type(label=value)
                top.shows = vi
        

        #dimensions
        dimensions = rec.get('dimensions','')
        if dimensions:
            top.referred_to_by = vocab.DimensionStatement(content=dimensions)

        #inscriptions
        ins = rec.get('inscriptions')
        if ins:
            top.referred_to_by = vocab.InscriptionStatement(content=ins)

        #exhibitions, publications, provenance
        exhibition_history = rec.get('exhibition_history','')
        if exhibition_history:
            top.referred_to_by = vocab.ExhibitionStatement(content=exhibition_history)
        with open('/Users/kd736/Desktop/artic/data/artic_exhibitions.json','r') as j:
            try:
                exhibitionids = json.loads(j.read())
                exhid = exhibitionids[str(rec['id'])]
            except:
                exhid = None
        if exhid:
            top.member_of = model.Set(ident=f'https://api.artic.edu/api/v1/exhibitions/{str(exhid)}')
        publication_history = rec.get('publication_history','')
        if publication_history:
            top.referred_to_by = vocab.BibliographyStatement(content=publication_history)
        provenance = rec.get('provenance_text','')
        if provenance:
            top.referred_to_by = vocab.ProvenanceStatement(content=provenance)

        #credit statement
        credit_statement = rec.get('credit_line','')
        if credit_statement:
            top.referred_to_by = vocab.CreditStatement(content=credit_statement)

        #object type
        type_title = rec.get('artwork_type_title')
        artwork_type_id = rec.get('artwork_type_id')
        with open('/Users/kd736/Desktop/artic/data/artic_artwork_aats.json','r') as j:
            try:
                aat_ids = json.loads(j.read())
                aat_id = aat_ids[str(artwork_type_id)]
            except:
                aat_id = None
        if aat_id:
            what = model.Type(ident=f"https://vocab.getty.edu/aat/{str(aat_id)}", label=type_title)
            what.classified_as = vocab.instances['work type']
            #vocab.instances['work type'] is not callable
            top.classified_as = what

        #department
        dept = rec.get('department_title','')
        with open('/Users/kd736/Desktop/artic/data/depts.json') as file:
            depts = json.loads(file.read())
            try:
                dept_URI = depts[dept]
            except:
                dept_URI = None
        if dept and dept_URI:
            top.member_of = model.Set(ident=dept_URI,label=dept)
        
        #collection webpage
        textOfPage = model.LinguisticObject()
        top.subject_of = textOfPage
        wpage = vocab.WebPage()
        textOfPage.digitally_carried_by = wpage
        wpage.access_point = model.DigitalObject(ident=f"https://www.artic.edu/artworks/{str(rec['id'])}")

    def handle_activity(self, rec, top, topcls):
        #exhibitions webpage
        weburl = rec.get('web_url','')
        if weburl:
            textOfPage = model.LinguisticObject()
            top.subject_of = textOfPage
            wpage = vocab.WebPage()
            textOfPage.digitally_carried_by = wpage
            wpage.access_point = model.DigitalObject(ident=weburl)

        #dates
        displayDate = rec.get('date_display','')
        beginDate = rec.get('aic_start_at','')
        endDate = rec.get('aic_end_at','')
        if beginDate and endDate:
            self.handle_timespan(top, beginDate, endDate, displayDate)

        #exhibition set
        top.used_specific_object = model.Set(ident=f"https://api.artic.edu/api/v1/exhibitions/{str(rec['id'])}")

        #galleries
        gal_id = rec.get('gallery_id')
        with open('/Users/kd736/Desktop/artic/data/galleries.json') as gj:
            galleries = json.loads(gj.read())
            try:
                gal_title = galleries[gal_id]
            except:
                gal_title = None
        if gal_id and gal_title:
            top.took_place_at = model.Place(ident=f'https://api.artic.edu/api/v1/galleries/{str(gal_id)}', label=gal_title)

        #sounds, images, texts
        doc_ids = rec.get('document_ids',[])
        self.handle_documents(doc_ids,top)


        #exhibition related events
        title = rec.get('title','')
        with open('/Users/kd736/Desktop/artic/data/events.json','r') as je:
            events = json.loads(je.read())
            event = None 
            for key, value in events.items():
                event_title = value[0]
                event_desc = value[1]
                event_loc = value[2]
                start_date = value[3]
                end_date = value[4]
                rsvp_link = value[5]
                event_api = value[-1]
                if title in event_title:
                    event = key
                    break
        if event:
            exh_event = model.Activity(ident=event_api)
            if event_loc:
                exh_event.took_place_at = model.Place(label=event_loc)
            exh_event.identified_by = vocab.PrimaryName(content=event_title)
            pageText = model.LinguisticObject(content=event_desc)
            web_page = vocab.WebPage()
            if rsvp_link:
                web_page.access_point = model.DigitalObject(ident=rsvp_link)
            pageText.digitally_carried_by = web_page
            exh_event.subject_of = pageText
            if start_date:
                self.handle_timespan(exh_event, start_date)
            top.caused = exh_event

        #printed catalogs
        with open('/Users/kd736/Desktop/artic/data/printedcats.json','r') as jpc:
            printedcats = json.loads(jpc.read())
            printedcat = None 
            for k,v in printedcats.items():
                cat_title = v[0]
                web_url = v[1]
                copy = v[2]
                if title == cat_title:
                    printedcat = k
                    break
        if printedcat:        
            lo = vocab.ExhibitionCatalogText(content=copy)
            web_do = vocab.WebPage()
            web_do.access_point = model.DigitalObject(ident=web_url)
            lo.digitally_carried_by = web_do
            top.referred_to_by = lo

        #press release
        t = title.lower()
        with open('/Users/kd736/Desktop/artic/data/artic_press-releases.json','r') as jpr:
            releases = json.loads(jpr.read())
            release = None
            for k,v in releases.items():
                web_url = v[2]
                press_title = v[0].lower()
                copy = v[1]
                if press_title == f"the art institute presents {t}":
                    release = k
                    break
                elif press_title == f"the art institute of chicago presents {t}":
                    release = k
                    break
                elif press_title == t:
                    release = k
                    break 
        if release:
            lo = model.LinguisticObject(content=copy)
            lo.classified_as = model.Type(ident='https://vocab.getty.edu/aat/300026437',label='press release')
            web_do = vocab.WebPage()
            web_do.access_point = model.DigitalObject(ident=web_url)
            lo.digitally_carried_by = web_do
            top.referred_to_by = lo

        #articles
        with open('/Users/kd736/Desktop/artic/data/artic_articles.json','r') as j:
            articles = json.loads(j.read())
            article = None 
            for k,v in articles.items():
                if title in v:
                    article = k
                    copy = v
                else:
                    pass
        if article:
            lo = vocab.ArticleText(content=copy)
            web_do = vocab.WebPage()
            web_do.access_point = model.DigitalObject(ident=f'https://api.artic.edu/api/v1/articles/{article}')
            lo.digitally_carried_by = web_do
            top.referred_to_by = lo