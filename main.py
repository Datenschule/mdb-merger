import logging
import json
import click
from normdatei.text import fingerprint, clean_name
from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, scoped_session, relationship
from tqdm import tqdm
from Levenshtein import distance
from datetime import datetime


@click.command()
@click.option('--db_url', required=True, default='postgres://postgres@0.0.0.0:32780')
@click.option('--agw_path', type=click.Path(exists=True), required=True)
@click.option('--parliament_path', type=click.Path(exists=True), required=True)
@click.option('--job_dict_path', type=click.Path(exists=True), required=True)
@click.option('--start', type=click.INT, default=0)
@click.option('--end', type=click.INT, default=245)
def main(db_url, agw_path, parliament_path, job_dict_path, start, end):
    """Merge Utterances from a db with topics from a json file"""
    # cleaned_classes = simplify_classes(classes_path)
    # speaker = get_json_file(tops_path)
    # detail = get_json_file(session_path)
    # merged_tops = json_top_merge(speaker, detail, cleaned_classes)
    # write_json_file('data/merged.json', merged_tops)
    # if verbose:
    #     logging.basicConfig(level=logging.INFO)
    # init_sqlalchemy(dbname=db_url)
    # for i in tqdm(range(start, end)):
    #     try:
    #         # run_for(i, tops_path)
    #         run_for(i, 'data/merged.json')
    #     except Exception as e:
    #         print("Failed with {} for {}".format(e, i))
    print(db_url)
    print(agw_path)
    print(parliament_path)
    merged = merge_json_data(agw_path, parliament_path)
    init_sqlalchemy(db_url)

    with open(job_dict_path) as job_file:
        jobs = json.load(job_file)
    update_mdb(merged, jobs)

    all_mdb = DBSession.query(MdB).all()
    for i in tqdm(range(start, end)):
        try:
            # run_for(i, tops_path)
            update_utterance(i, all_mdb)
        except Exception as e:
            print("Failed with {} for {}".format(e, i))

Base = declarative_base()
DBSession = scoped_session(sessionmaker())
engine = None


class Utterance(Base):
    __tablename__ = "de_bundestag_plpr"

    id = Column(Integer, primary_key=True)
    wahlperiode = Column(Integer)
    sitzung = Column(Integer)
    sequence = Column(Integer)
    speaker_cleaned = Column(String)
    speaker_party = Column(String)
    speaker = Column(String)
    speaker_fp = Column(String)
    speaker_id = Column(String)
    type = Column(String)
    text = Column(String)
    top_id = Column(Integer)
    #top = relationship("Top")
    speaker_key = Column(Integer)

    @staticmethod
    def get_all(wahlperiode, sitzung, session):
        return session.query(Utterance) \
            .filter(Utterance.sitzung == sitzung) \
            .filter(Utterance.wahlperiode == wahlperiode) \
            .order_by(Utterance.sequence) \
            .all()

    def __repr__(self):
        return '<Utterance {}-{}-{}>'.format(self.wahlperiode, self.sitzung, self.sequence)


class Top(Base):
    __tablename__ = "tops"
    id = Column(Integer, primary_key=True)
    wahlperiode = Column(Integer)
    sitzung = Column(Integer)
    title = Column(String)
    title_clean = Column(String)
    description = Column(String)
    number = Column(String)
    week = Column(Integer)
    detail = Column(String)
    year = Column(Integer)
    category = Column(String)

    def save(self):
        try:
            DBSession.add(self)
            DBSession.commit()
        except Exception as se:
            print(se)
            DBSession.rollback()

    @staticmethod
    def delete_for_session(wahlperiode, sitzung):
        DBSession.query(Top) \
            .filter_by(wahlperiode=wahlperiode) \
            .filter_by(sitzung=sitzung) \
            .delete()

class MdB(Base):
    __tablename__ = "mdb"

    id = Column(Integer, primary_key=True)
    profile_url = Column(String)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    birth_date = Column(Date)
    education = Column(String)
    picture = Column(String)
    party = Column(String)
    election_list = Column(String)
    list_won = Column(String)
    agw_id = Column(String)
    education_category = Column(String)

    @staticmethod
    def get_all():
        return db.session.query(MdB) \
            .all()

    def save(self):
        try:
            DBSession.add(self)
            DBSession.commit()
        except Exception as se:
            print(se)
            DBSession.rollback()

    def __repr__(self):
        return '<MdB {}-{}-{}>'.format(self.first_name, self.last_name, self.party)


def init_sqlalchemy(dbname):
    global engine
    engine = create_engine(dbname, echo=False)
    DBSession.remove()
    DBSession.configure(bind=engine, autoflush=False, expire_on_commit=False)

def fingerclean_birthdate(prename, surname):
    success = False
    result = None
    if prename == "Claudia" and surname == "Tausend":
        success = True
        result = datetime.strptime('1964-07-22', '%Y-%m-%d')
    elif prename == "Britta" and surname == "Haßelmann":
        success = True
        result = datetime.strptime('1961-12-10', '%Y-%m-%d')

    if success:
        print("Successful fingerclean for {} {}".format(prename, surname))
    else:
        print("Could not fingerclean {} {}".format(prename, surname))

    return result

def update_mdb(data, jobs):
    DBSession.query(MdB).delete()

    for item in data:
        birth_date = None

        if 'birth_date' in item['parl'].keys():
            birth_date = datetime.strptime(item['parl']['birth_date'], '%Y-%m-%d')
        else:
            print ('No birth date for {} {}'.format(item['agw']['personal.first_name'], item['agw']['personal.last_name']))
            birth_date = fingerclean_birthdate(item['agw']['personal.first_name'],  item['agw']['personal.last_name'])

        education = item['agw']['personal.education']

        mdb = MdB(
            agw_id=item['agw']['list.uuid'],
            profile_url=item['agw']['meta.url'],
            first_name=item['agw']['personal.first_name'],
            last_name=item['agw']['personal.last_name'],
            gender=item['agw']['personal.gender'],
            birth_date= birth_date,
            education=education,
            picture=item['parl']['image'],
            party=item['agw']['party'],
            election_list=item['agw']['list.name'],
            list_won=True if item['agw']['constituency.won'] == "true" else False,
            education_category=jobs[education] if education in jobs.keys() else None
        )
        mdb.save()
    print('saved all mdb')

def update_utterance(session, all_mdb):
    utterances = Utterance.get_all(18, session, DBSession)

    for utterance in utterances:
        if utterance.speaker_cleaned != '' and utterance.speaker_cleaned != None:
            mdb = list(filter(lambda x: fingerprint(x.first_name + ' ' + x.last_name) == utterance.speaker_fp, all_mdb))
            if len(mdb) > 1:
                print("Error comparing MdB")
            elif len(mdb) == 1:
                utterance.speaker_key = mdb[0].id

    DBSession.bulk_save_objects(utterances)
    DBSession.commit()

    print('loaded all Utterances')


def run_for(SESSION, tops_path):
    utterances = Utterance.get_all(18, SESSION, DBSession)
    plpr = get_speaker_sequence(utterances)
    json_data = get_json(tops_path, SESSION)

    results = []
    offset = 0
    for index, entry in enumerate(json_data):
        cleaned_top_speaker = fingerclean(entry['speaker'])
        cleaned_protocol_speaker = fingerclean(plpr[index + offset].speaker_cleaned)
        while distance(cleaned_protocol_speaker, cleaned_top_speaker) > 3:
            logging.info('Comparing: %s ... %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
            offset += 1
            cleaned_top_speaker = fingerclean(entry['speaker'])
            cleaned_protocol_speaker = fingerclean(plpr[index + offset].speaker_cleaned)
        logging.info('Match: %s -> %s', entry['speaker'], plpr[index + offset].speaker_cleaned)
        if not results or results[-1]['topic'] != entry['top']:
            results.append({'sequence': plpr[index + offset].sequence, 'topic': entry['top'], 'top_obj': entry['top_obj']})

    update_utterances(utterances, results, SESSION)

    DBSession.bulk_save_objects(utterances)
    DBSession.commit()

def merge_json_data(agw_path, parl_path):

    with open(agw_path) as agw_file:
        agw_data = json.load(agw_file)

    with open(parl_path) as parl_file:
        parl_data = json.load(parl_file)

    result = []

    for mdb in agw_data:
        found = False
        for parl_mdb in parl_data['persons']:
            parl_honory = ''
            if 'honorific_prefix' in parl_mdb.keys():
                parl_honory = parl_mdb['honorific_prefix'] + ' '
            mdb_name = fingerclean_mdb_name('{} {}'.format(mdb['personal.first_name'], mdb['personal.last_name']))
            parl_name = '{} {}{}'.format(parl_mdb['given_name'], parl_honory, parl_mdb['family_name'])
            # print('comparing mdb:{} with parl:', mdb_name, parl_name)
            if mdb_name == parl_name:
                if (found == False):
                    result.append({'agw': mdb, 'parl': parl_mdb})
                    found = True
                else:
                    print('found multiples for {} {}'.format(mdb['personal.first_name'], mdb['personal.last_name']))
        if (found == False):
            print('nothing found for {} {}'.format(mdb['personal.first_name'], mdb['personal.last_name']))

    return result


def fingerclean_mdb_name(value):
    if value == "Bernd-Bernhard Fabritius":
        return "Bernd Fabritius"
    elif value == "Karl-Heinz (Charles M.) Huber":
        return "Charles M. Huber"
    elif value == "Aydan Özoguz":
        return "Aydan Özoğuz"
    elif value == "Stefan Dr. Heck":
        return "Stefan Heck"
    elif value == "Chris Kühn":
        return "Christian Kühn"
    elif value == "Philipp Graf von und zu Lerchenfeld":
        return "Philipp Graf Lerchenfeld"
    elif value == "Dagmar Wöhrl":
        return "Dagmar G. Wöhrl"
    elif value == "Dipl.-Soz.Wiss. Matthias W. Birkwald":
        return "Matthias W. Birkwald"
    elif value == "Ernst-Dieter Rossmann":
        return "Ernst Dieter Rossmann"
    elif value == "Ulrike (Ulli) Nissen":
        return "Ulli Nissen"
    elif value == "Andreas Lämmel":
        return "Andreas G. Lämmel"
    elif value == "Ulrich Wolfgang Kelber":
        return "Ulrich Kelber"
    elif value == "Franz-Josef Jung":
        return "Franz Josef Jung"
    elif value == "Sevim Dagdelen":
        return "Sevim Dağdelen"
    elif value == "Helmut Günter Baumann":
        return "Günter Baumann"
    else:
        return value


def update_utterances(utterances, results, session):
    last_utterance = 0
    Top.delete_for_session(18, session)
    for index in range(len(results)):
        current_top = results[index]
        try:
            next_top = results[index + 1]
        except IndexError:
            next_top = {'sequence': 100000000}
        top_obj = current_top['top_obj'];
        top = Top(wahlperiode=18,
                  sitzung=session,
                  title=current_top['topic'],
                  category=";".join(top_obj['categories']),
                  description=top_obj['description'] if 'description' in top_obj.keys() else None,
                  detail=top_obj['detail'] if 'detail' in top_obj.keys() else None,
                  number=top_obj['number'] if 'number' in top_obj.keys() else None,
                  title_clean=top_obj['title_clean'] if 'title_clean' in top_obj.keys() else None,
                  week=top_obj['week'] if 'week' in top_obj.keys() else None,
                  year=top_obj['year'] if 'year' in top_obj.keys() else None
                  )
        top.save()
        for u in utterances[last_utterance:]:
            if u.sequence < next_top['sequence']:
                u.top_id = top.id
            else:
                last_utterance = u.sequence
                break


if __name__ == "__main__":
    main()
