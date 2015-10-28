#!/usr/bin/env python

"""
conference.py -- Udacity conference server-side Python App Engine API;
    uses Google Cloud Endpoints

$Id: conference.py,v 1.25 2014/05/24 23:42:19 wesc Exp wesc $

created by wesc on 2014 apr 21

"""

__author__ = 'wesc+api@google.com (Wesley Chun)'


from datetime import datetime

import endpoints
from protorpc import messages
from protorpc import message_types
from protorpc import remote

from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
from google.appengine.ext.db import BadValueError

from models import ConflictException
from models import Profile
from models import ProfileMiniForm
from models import ProfileForm
from models import StringMessage
from models import BooleanMessage
from models import Conference
from models import ConferenceForm
from models import ConferenceForms, MarketableConferenceForm
from models import MarketableConferenceForms
from models import ConferenceQueryForm
from models import ConferenceQueryForms
from models import TeeShirtSize
from models import Session, SessionForm, SessionForms
from models import SessionQueryForm, SessionQueryForms
from models import Speaker, SpeakerForm, SpeakerForms
from models import SpeakerFormWsk, SpeakerFormsWsk

from settings import WEB_CLIENT_ID
from settings import ANDROID_CLIENT_ID
from settings import IOS_CLIENT_ID
from settings import ANDROID_AUDIENCE

from utils import getUserId

EMAIL_SCOPE = endpoints.EMAIL_SCOPE
API_EXPLORER_CLIENT_ID = endpoints.API_EXPLORER_CLIENT_ID
MEMCACHE_ANNOUNCEMENTS_KEY = "RECENT_ANNOUNCEMENTS"
ANNOUNCEMENT_TPL = ('Last chance to attend! The following conferences '
                    'are nearly sold out: %s')
MEMCACHE_FEATUREDSPEAKER_KEY = "FEATURED_SPEAKER"
FEATUREDSPEAKER_ANNOUNCEMENT = ('Featured Speaker at %s: %s is speaking in the following sessions: %s')
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

DEFAULTS = {
    "city": "Default City",
    "maxAttendees": 0,
    "seatsAvailable": 0,
    "topics": [ "Default", "Topic" ],
}

OPERATORS = {
            'EQ':   '=',
            'GT':   '>',
            'GTEQ': '>=',
            'LT':   '<',
            'LTEQ': '<=',
            'NE':   '!='
            }

FIELDS =    {
            'CITY': 'city',
            'TOPIC': 'topics',
            'MONTH': 'month',
            'MAX_ATTENDEES': 'maxAttendees',
            }

CONF_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
)

CONF_POST_REQUEST = endpoints.ResourceContainer(
    ConferenceForm,
    websafeConferenceKey=messages.StringField(1),
)

SESSION_POST_REQUEST = endpoints.ResourceContainer(
    SessionForm,
    websafeConferenceKey=messages.StringField(1)
)

SESSION_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1)
)

SESSION_BY_TYPE_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    websafeConferenceKey=messages.StringField(1),
    typeOfSession=messages.StringField(2)
)

SESSION_BY_SPEAKER_GET_REQUEST = endpoints.ResourceContainer(
    message_types.VoidMessage,
    speaker=messages.StringField(1)
)

WHISHLIST_POST_REQUEST = endpoints.ResourceContainer(
    SessionKey=messages.StringField(1)
)

SPEAKERS_GET_REQUEST = endpoints.ResourceContainer(
    websafeConferenceKey=messages.StringField(1),
)

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


@endpoints.api(name='conference', version='v1', audiences=[ANDROID_AUDIENCE],
    allowed_client_ids=[WEB_CLIENT_ID, API_EXPLORER_CLIENT_ID, ANDROID_CLIENT_ID, IOS_CLIENT_ID],
    scopes=[EMAIL_SCOPE])
class ConferenceApi(remote.Service):
    """Conference API v0.1"""

# - - - Conference objects - - - - - - - - - - - - - - - - -

    def _copyConferenceToForm(self, conf, displayName):
        """Copy relevant fields from Conference to ConferenceForm."""
        cf = ConferenceForm()
        for field in cf.all_fields():
            if hasattr(conf, field.name):
                # convert Date to date string; just copy others
                if field.name.endswith('Date'):
                    setattr(cf, field.name, str(getattr(conf, field.name)))
                else:
                    setattr(cf, field.name, getattr(conf, field.name))
            elif field.name == "websafeKey":
                setattr(cf, field.name, conf.key.urlsafe())
        if displayName:
            setattr(cf, 'organizerDisplayName', displayName)
        cf.check_initialized()
        return cf


    def _createConferenceObject(self, request):
        """Create or update Conference object, returning ConferenceForm/request."""
        # preload necessary data items
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        if not request.name:
            raise endpoints.BadRequestException("Conference 'name' field required")

        # copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}
        del data['websafeKey']
        del data['organizerDisplayName']

        # add default values for those missing (both data model & outbound Message)
        for df in DEFAULTS:
            if data[df] in (None, []):
                data[df] = DEFAULTS[df]
                setattr(request, df, DEFAULTS[df])

        # convert dates from strings to Date objects; set month based on start_date
        if data['startDate']:
            data['startDate'] = datetime.strptime(data['startDate'][:10], "%Y-%m-%d").date()
            data['month'] = data['startDate'].month
        else:
            data['month'] = 0
        if data['endDate']:
            data['endDate'] = datetime.strptime(data['endDate'][:10], "%Y-%m-%d").date()

        # set seatsAvailable to be same as maxAttendees on creation
        if data["maxAttendees"] > 0:
            data["seatsAvailable"] = data["maxAttendees"]
        # generate Profile Key based on user ID and Conference
        # ID based on Profile key get Conference key from ID
        p_key = ndb.Key(Profile, user_id)
        c_id = Conference.allocate_ids(size=1, parent=p_key)[0]
        c_key = ndb.Key(Conference, c_id, parent=p_key)
        data['key'] = c_key
        data['organizerUserId'] = request.organizerUserId = user_id

        # create Conference, send email to organizer confirming
        # creation of Conference & return (modified) ConferenceForm
        Conference(**data).put()
        taskqueue.add(params={'email': user.email(),
            'conferenceInfo': repr(request)},
            url='/tasks/send_confirmation_email'
        )
        return request


    @ndb.transactional()
    def _updateConferenceObject(self, request):
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # copy ConferenceForm/ProtoRPC Message into dict
        data = {field.name: getattr(request, field.name) for field in request.all_fields()}

        # update existing conference
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        # check that conference exists
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)

        # check that user is owner
        if user_id != conf.organizerUserId:
            raise endpoints.ForbiddenException(
                'Only the owner can update the conference.')

        # Not getting all the fields, so don't create a new object; just
        # copy relevant fields from ConferenceForm to Conference object
        for field in request.all_fields():
            data = getattr(request, field.name)
            # only copy fields where we get data
            if data not in (None, []):
                # special handling for dates (convert string to Date)
                if field.name in ('startDate', 'endDate'):
                    data = datetime.strptime(data, "%Y-%m-%d").date()
                    if field.name == 'startDate':
                        conf.month = data.month
                # write to Conference object
                setattr(conf, field.name, data)
        conf.put()
        prof = ndb.Key(Profile, user_id).get()
        return self._copyConferenceToForm(conf, getattr(prof, 'displayName'))

    @endpoints.method(ConferenceForm,
                      ConferenceForm,
                      path='conference',
                      http_method='POST',
                      name='createConference')
    def createConference(self, request):
        """Create new conference."""
        return self._createConferenceObject(request)

    @endpoints.method(CONF_POST_REQUEST,
                      ConferenceForm,
                      path='conference/{websafeConferenceKey}',
                      http_method='PUT',
                      name='updateConference')
    def updateConference(self, request):
        """Update conference w/provided fields & return w/updated info."""
        return self._updateConferenceObject(request)


    @endpoints.method(CONF_GET_REQUEST, ConferenceForm,
            path='conference/{websafeConferenceKey}',
            http_method='GET', name='getConference')
    def getConference(self, request):
        """Return requested conference (by websafeConferenceKey)."""
        # get Conference object from request; bail if not found
        conf = ndb.Key(urlsafe=request.websafeConferenceKey).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % request.websafeConferenceKey)
        prof = conf.key.parent().get()
        # return ConferenceForm
        return self._copyConferenceToForm(conf, getattr(prof, 'displayName'))


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='getConferencesCreated',
            http_method='POST', name='getConferencesCreated')
    def getConferencesCreated(self, request):
        """Return conferences created by user."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # create ancestor query for all key matches for this user
        confs = Conference.query(ancestor=ndb.Key(Profile, user_id))
        prof = ndb.Key(Profile, user_id).get()
        # return set of ConferenceForm objects per Conference
        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, getattr(prof, 'displayName')) for conf in confs]
        )


    def _getQuery(self, request):
        """Return formatted query from the submitted filters."""
        q = Conference.query()
        inequality_filter, filters = self._formatFilters(request.filters)

        # If exists, sort on inequality filter first
        if not inequality_filter:
            q = q.order(Conference.name)
        else:
            q = q.order(ndb.GenericProperty(inequality_filter))
            q = q.order(Conference.name)

        for filtr in filters:
            if filtr["field"] in ["month", "maxAttendees"]:
                filtr["value"] = int(filtr["value"])
            formatted_query = ndb.query.FilterNode(filtr["field"], filtr["operator"], filtr["value"])
            q = q.filter(formatted_query)
        return q


    def _formatFilters(self, filters):
        """Parse, check validity and format user supplied filters."""
        formatted_filters = []
        inequality_field = None

        for f in filters:
            filtr = {field.name: getattr(f, field.name) for field in f.all_fields()}

            try:
                filtr["field"] = FIELDS[filtr["field"]]
                filtr["operator"] = OPERATORS[filtr["operator"]]
            except KeyError:
                raise endpoints.BadRequestException("Filter contains invalid field or operator.")

            # Every operation except "=" is an inequality
            if filtr["operator"] != "=":
                # check if inequality operation has been used in previous filters
                # disallow the filter if inequality was performed on a different field before
                # track the field on which the inequality operation is performed
                if inequality_field and inequality_field != filtr["field"]:
                    raise endpoints.BadRequestException("Inequality filter is allowed on only one field.")
                else:
                    inequality_field = filtr["field"]

            formatted_filters.append(filtr)
        return (inequality_field, formatted_filters)


    @endpoints.method(ConferenceQueryForms, ConferenceForms,
            path='queryConferences',
            http_method='POST',
            name='queryConferences')
    def queryConferences(self, request):
        """Query for conferences."""
        conferences = self._getQuery(request)

        # need to fetch organiser displayName from profiles
        # get all keys and use get_multi for speed
        organisers = [(ndb.Key(Profile, conf.organizerUserId)) for conf in conferences]
        profiles = ndb.get_multi(organisers)

        # put display names in a dict for easier fetching
        names = {}
        for profile in profiles:
            names[profile.key.id()] = profile.displayName

        # return individual ConferenceForm object per Conference
        return ConferenceForms(
                items=[self._copyConferenceToForm(conf, names[conf.organizerUserId]) for conf in \
                conferences]
        )


# - - - Profile objects - - - - - - - - - - - - - - - - - - -

    def _copyProfileToForm(self, prof):
        """Copy relevant fields from Profile to ProfileForm."""
        # copy relevant fields from Profile to ProfileForm
        pf = ProfileForm()
        for field in pf.all_fields():
            if hasattr(prof, field.name):
                # convert t-shirt string to Enum; just copy others
                if field.name == 'teeShirtSize':
                    setattr(pf, field.name, getattr(TeeShirtSize, getattr(prof, field.name)))
                else:
                    setattr(pf, field.name, getattr(prof, field.name))
        pf.check_initialized()
        return pf


    def _getProfileFromUser(self):
        """Return user Profile from datastore, creating new one if non-existent."""
        # make sure user is authed
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # get Profile from datastore
        user_id = getUserId(user)
        p_key = ndb.Key(Profile, user_id)
        profile = p_key.get()
        # create new Profile if not there
        if not profile:
            profile = Profile(
                key = p_key,
                displayName = user.nickname(),
                mainEmail= user.email(),
                teeShirtSize = str(TeeShirtSize.NOT_SPECIFIED),
            )
            profile.put()

        return profile      # return Profile


    def _doProfile(self, save_request=None):
        """Get user Profile and return to user, possibly updating it first."""
        # get user Profile
        prof = self._getProfileFromUser()

        # if saveProfile(), process user-modifyable fields
        if save_request:
            for field in ('displayName', 'teeShirtSize'):
                if hasattr(save_request, field):
                    val = getattr(save_request, field)
                    if val:
                        setattr(prof, field, str(val))
                        #if field == 'teeShirtSize':
                        #    setattr(prof, field, str(val).upper())
                        #else:
                        #    setattr(prof, field, val)
                        prof.put()

        # return ProfileForm
        return self._copyProfileToForm(prof)


    @endpoints.method(message_types.VoidMessage, ProfileForm,
            path='profile', http_method='GET', name='getProfile')
    def getProfile(self, request):
        """Return user profile."""
        return self._doProfile()


    @endpoints.method(ProfileMiniForm, ProfileForm,
            path='profile', http_method='POST', name='saveProfile')
    def saveProfile(self, request):
        """Update & return user profile."""
        return self._doProfile(request)


# - - - Announcements - - - - - - - - - - - - - - - - - - - -

    @staticmethod
    def _cacheAnnouncement():
        """Create Announcement & assign to memcache; used by
        memcache cron job & putAnnouncement().
        """
        confs = Conference.query(ndb.AND(
            Conference.seatsAvailable <= 5,
            Conference.seatsAvailable > 0)
        ).fetch(projection=[Conference.name])

        if confs:
            # If there are almost sold out conferences,
            # format announcement and set it in memcache
            announcement = ANNOUNCEMENT_TPL % (
                ', '.join(conf.name for conf in confs))
            memcache.set(MEMCACHE_ANNOUNCEMENTS_KEY, announcement)
        else:
            # If there are no sold out conferences,
            # delete the memcache announcements entry
            announcement = ""
            memcache.delete(MEMCACHE_ANNOUNCEMENTS_KEY)

        return announcement


    @endpoints.method(message_types.VoidMessage, StringMessage,
            path='conference/announcement/get',
            http_method='GET', name='getAnnouncement')
    def getAnnouncement(self, request):
        """Return Announcement from memcache."""
        return StringMessage(data=memcache.get(MEMCACHE_ANNOUNCEMENTS_KEY) or "")


# - - - Registration - - - - - - - - - - - - - - - - - - - -

    @ndb.transactional(xg=True)
    def _conferenceRegistration(self, request, reg=True):
        """Register or unregister user for selected conference."""
        retval = None
        prof = self._getProfileFromUser() # get user Profile

        # check if conf exists given websafeConfKey
        # get conference; check that it exists
        wsck = request.websafeConferenceKey
        conf = ndb.Key(urlsafe=wsck).get()
        if not conf:
            raise endpoints.NotFoundException(
                'No conference found with key: %s' % wsck)

        # register
        if reg:
            # check if user already registered otherwise add
            if wsck in prof.conferenceKeysToAttend:
                raise ConflictException(
                    "You have already registered for this conference")

            # check if seats avail
            if conf.seatsAvailable <= 0:
                raise ConflictException(
                    "There are no seats available.")

            # register user, take away one seat
            prof.conferenceKeysToAttend.append(wsck)
            conf.seatsAvailable -= 1
            retval = True

        # unregister
        else:
            # check if user already registered
            if wsck in prof.conferenceKeysToAttend:

                # unregister user, add back one seat
                prof.conferenceKeysToAttend.remove(wsck)
                conf.seatsAvailable += 1
                retval = True
            else:
                retval = False

        # write things back to the datastore & return
        prof.put()
        conf.put()
        return BooleanMessage(data=retval)


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='conferences/attending',
            http_method='GET', name='getConferencesToAttend')
    def getConferencesToAttend(self, request):
        """Get list of conferences that user has registered for."""
        prof = self._getProfileFromUser() # get user Profile
        conf_keys = [ndb.Key(urlsafe=wsck) for wsck in prof.conferenceKeysToAttend]
        conferences = ndb.get_multi(conf_keys)

        # get organizers
        organisers = [ndb.Key(Profile, conf.organizerUserId) for conf in conferences]
        profiles = ndb.get_multi(organisers)

        # put display names in a dict for easier fetching
        names = {}
        for profile in profiles:
            names[profile.key.id()] = profile.displayName

        # return set of ConferenceForm objects per Conference
        return ConferenceForms(items=[self._copyConferenceToForm(conf, names[conf.organizerUserId])\
         for conf in conferences]
        )


    @endpoints.method(CONF_GET_REQUEST, BooleanMessage,
            path='conference/{websafeConferenceKey}',
            http_method='POST', name='registerForConference')
    def registerForConference(self, request):
        """Register user for selected conference."""
        return self._conferenceRegistration(request)


    @endpoints.method(CONF_GET_REQUEST, BooleanMessage,
            path='conference/{websafeConferenceKey}',
            http_method='DELETE', name='unregisterFromConference')
    def unregisterFromConference(self, request):
        """Unregister user for selected conference."""
        return self._conferenceRegistration(request, reg=False)


    @endpoints.method(message_types.VoidMessage, ConferenceForms,
            path='filterPlayground',
            http_method='GET', name='filterPlayground')
    def filterPlayground(self, request):
        """Filter Playground"""
        q = Conference.query()
        # field = "city"
        # operator = "="
        # value = "London"
        # f = ndb.query.FilterNode(field, operator, value)
        # q = q.filter(f)
        q = q.filter(Conference.city == "London")
        q = q.filter(Conference.topics == "Medical Innovations")
        q = q.filter(Conference.month == 6)

        return ConferenceForms(
            items=[self._copyConferenceToForm(conf, "") for conf in q]
        )


# - - - Speaker - - - - - - - - - - - - - - - - - - - -

    @endpoints.method(SpeakerForm,
                      SpeakerForm,
                      path='speaker',
                      http_method='POST',
                      name='createSpeaker')
    def createSpeaker(self, request):
        """Create a speaker"""
        spkr = Speaker(name=request.name)
        spkr.put()
        return SpeakerForm(name=getattr(spkr, 'name'))

    @endpoints.method(message_types.VoidMessage,
                      SpeakerFormsWsk,
                      path='speaker/all',
                      http_method='GET',
                      name='getAllSpeakers')
    def getAllSpeakers(self, request):
        """List all speakers"""
        spkrs = Speaker.query()
        return SpeakerFormsWsk(items=[
            SpeakerFormWsk(name=getattr(data, 'name'),
                           websafeKey=data.key.urlsafe())
            for data in spkrs])

    # Task 3, Item 2
    @endpoints.method(SPEAKERS_GET_REQUEST, SpeakerForms,
                      path='conference/{websafeConferenceKey}/speakers',
                      http_method='GET',
                      name='getConferenceSpeakers')
    def getConferenceSpeakers(self, request):
        """Return a list of all the speakers at a conference."""
        conference_key = ndb.Key(urlsafe=request.websafeConferenceKey)
        sessions = Session.query(ancestor=conference_key).fetch()
        # Get rid of duplicates
        speakers_unique = set([session.speakerKey for session in sessions])
        speakers = ndb.get_multi(speakers_unique)
        return SpeakerForms(items=[SpeakerForm(name=getattr(data, 'name'))
                                   for data in speakers])

# - - - Sessions - - - - - - - - - - - - - - - - - - - -

    def _makeSessionQueryForm(self, session_obj):
        """Populates a session form"""
        return SessionQueryForm(name=getattr(session_obj, 'name'),
                                highlight=getattr(session_obj, 'highlight'),
                                speakerKey=getattr(session_obj,
                                                   'speakerKey').get().name,
                                duration=getattr(session_obj, 'duration'),
                                sessionType=getattr(session_obj, 'sessionType'),
                                date=str(getattr(session_obj, 'date')),
                                startTime=str(
                                    getattr(session_obj, 'startTime')),
                                location=getattr(session_obj, 'location'),
                                websafeSessionKey=session_obj.key.urlsafe())

    @endpoints.method(SESSION_POST_REQUEST,
                      SessionQueryForm,
                      path='conference/session',
                      http_method='POST',
                      name='createSession')
    def createSession(self, request):
        """Create a session for a specific conference"""
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')
        user_id = getUserId(user)

        # Verify all fields have values
        for field in request.all_fields():
            if not getattr(request, field.name):
                raise endpoints.BadRequestException(
                    "%s is a required field" % field.name)

        # Get conference object from the websafe key
        conf_key = ndb.Key(urlsafe=request.websafeConferenceKey)
        conf_obj = conf_key.get()
        p_key = ndb.Key(Profile, user_id)
        if not p_key != conf_obj.organizerUserId:
            raise endpoints.UnauthorizedException('Cannot add Session to Conference: Both creator profiles need to match')

        # Move data over from request to dictionary for easy processing
        data = {field.name: getattr(request, field.name)
                for field in request.all_fields()}
        data['speakerKey'] = ndb.Key(urlsafe=data['speakerKey'])
        data["date"] = datetime.strptime(data["date"], "%Y-%m-%d").date()
        data["startTime"] = datetime.strptime(data["startTime"], "%H:%M").time()
        del data['websafeConferenceKey']

        # Save the session, with conference as ancestor.
        newsession_key = Session(parent=conf_key, **data).put()

        # Task 4: Count speaker engagements for this conference, and set
        # an announcement if needed
        taskqueue.add(url='/tasks/set_featuredspeaker',
                      params={'speaker_key': data['speakerKey'].urlsafe(),
                              'conference_key': request.websafeConferenceKey})

        return self._makeSessionQueryForm(newsession_key.get())

    @endpoints.method(SESSION_GET_REQUEST,
                      SessionQueryForms,
                      path='conference/{websafeConferenceKey}/session',
                      http_method='GET',
                      name='getConferenceSessions')
    def getConferenceSessions(self, request):
        """Returns all the sessions for a specific conference"""
        conference_key = ndb.Key(urlsafe=request.websafeConferenceKey)
        session_q = Session.query(ancestor=conference_key).fetch()
        return SessionQueryForms(items=[self._makeSessionQueryForm(session)
                                        for session in session_q])


    @endpoints.method(SESSION_BY_TYPE_GET_REQUEST,
                      SessionQueryForms,
                      path='conference/{websafeConferenceKey}/session/type/{typeOfSession}',
                      http_method='GET',
                      name='getConferenceSessionsByType')
    def getConferenceSessionsByType(self, request):
        """Given a conference, return all sessions of a specified type"""
        from models import SESSION_TYPE
        conference_key = ndb.Key(urlsafe=request.websafeConferenceKey)
        session_type = request.typeOfSession

        if session_type not in SESSION_TYPE:
            raise endpoints.BadRequestException("Unknown Session type")

        session_q = Session.query(ancestor=conference_key).filter(
            Session.sessionType == session_type)
        return SessionQueryForms(items=[self._makeSessionQueryForm(session)
                                        for session in session_q])

    @endpoints.method(SESSION_BY_SPEAKER_GET_REQUEST,
                      SessionQueryForms,
                      path='conference/session/speaker/{speaker}',
                      http_method='GET',
                      name='getSessionsBySpeaker')
    def getSessionsBySpeaker(self, request):
        """Given a speaker, return all sessions given by this
         particular speaker, across all conferences"""
        speaker_obj = ndb.Key(urlsafe=request.speaker)
        session_q = Session.query().filter(Session.speakerKey == speaker_obj)
        return SessionQueryForms(items=[self._makeSessionQueryForm(session)
                                        for session in session_q])

# - - - Wishlist - - - - - - - - - - - - - - - - - - - -

    @endpoints.method(WHISHLIST_POST_REQUEST,
                      BooleanMessage,
                      path='wishlist/{SessionKey}',
                      http_method='POST',
                      name='addSessionToWishlist')
    def addSessionToWishlist(self, request):
        """Adds the session to the user's list of sessions
        they are interested in attending"""
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # get Profile from datastore
        user_id = getUserId(user)
        profile = ndb.Key(Profile, user_id).get()
        session_key = request.SessionKey

        if session_key in profile.sessionWishList:
            raise endpoints.ConflictException('Session already on your wishlist')

        profile.sessionWishList.append(session_key)
        profile.put()
        return BooleanMessage(data=True)

    @endpoints.method(message_types.VoidMessage,
                      SessionQueryForms,
                      path='wishlist',
                      http_method='GET',
                      name='getSessionsInWishlist')
    def getSessionsInWishlist(self, request):
        """Query for all the sessions in a conference
        that the user is interested in"""
        user = endpoints.get_current_user()
        if not user:
            raise endpoints.UnauthorizedException('Authorization required')

        # get Profile from datastore
        user_id = getUserId(user)
        profile = ndb.Key(Profile, user_id).get()
        wishlist_keys = [ndb.Key(urlsafe=x) for x in profile.sessionWishList]

        if not wishlist_keys:
            raise endpoints.BadRequestException('No wishlist items')

        session_q = Session.query().filter(Session.key.IN(wishlist_keys))
        return SessionQueryForms(items=[self._makeSessionQueryForm(session)
                                        for session in session_q])

# - - - Task - - - - - - - - - - - - - - - - - - - -


    @staticmethod
    def _cacheFeaturedSpeaker(speaker_websafe_key, conference_websafe_key):
        """Create featured speaker announcement & assign to memcache; used by
        memcache cron job & putAnnouncement().
        """
        # Create keys
        speaker_key = ndb.Key(urlsafe=speaker_websafe_key)
        conference_key = ndb.Key(urlsafe=conference_websafe_key)

        sessions_q = Session.query(ancestor=conference_key).filter(
            Session.speakerKey == speaker_key)

        if sessions_q.count() > 1:
            # Getting all the required data.
            speaker = speaker_key.get()
            conference = conference_key.get()
            sessions_names = ', '.join([x.name for x in sessions_q])
            if speaker:
                announcement = FEATUREDSPEAKER_ANNOUNCEMENT % (
                    conference.name,
                    speaker.name,
                    sessions_names)
                memcache.set(MEMCACHE_FEATUREDSPEAKER_KEY, announcement)
            else:
                announcement = ""
                memcache.delete(MEMCACHE_FEATUREDSPEAKER_KEY)

    @endpoints.method(message_types.VoidMessage, StringMessage,
                      path='conference/fs_announcement/get',
                      http_method='GET',
                      name='getFeaturedSpeaker')
    def getFeaturedSpeaker(self, request):
        """Return Announcement from memcache."""
        return StringMessage(data=memcache.get(MEMCACHE_FEATUREDSPEAKER_KEY) or "")

    @endpoints.method(message_types.VoidMessage, MarketableConferenceForms,
                      path='conference/inprogress/get',
                      http_method='GET',
                      name='getMarketableConferences')
    def getMarketableConferences(self, request):
        """Find the conferences that is in progress and not yet fully booked"""
        mtoday = datetime.now().date()
        # Get conferences with seats available
        conf_inprogress_seatsavail = set(
            Conference.query().filter(
                Conference.seatsAvailable > 0).fetch(keys_only=True))
        # Get conferences where open end date is more or equal than today
        conf_inprogress_enddate = set(
            Conference.query().filter(Conference.endDate >= mtoday).fetch(
                keys_only=True))
        # Get conferences where open start date is less than or equal to today
        conf_inprogress_startdate = set(
            Conference.query().filter(Conference.startDate <= mtoday).fetch(
                keys_only=True))

        # return a set with conferences common in all queries
        conf_inprogress_keys = conf_inprogress_seatsavail\
                               & conf_inprogress_startdate\
                               & conf_inprogress_enddate
        conf_inprogress = ndb.get_multi(conf_inprogress_keys)

        return MarketableConferenceForms(items=[
            MarketableConferenceForm(name=getattr(data, "name"),
                                     topics=getattr(data, "topics"),
                                     city=getattr(data, "city"),
                                     seatsAvailable=getattr(data,
                                                            "seatsAvailable"),
                                     endDate=str(getattr(data, "endDate")))
            for data in conf_inprogress])

api = endpoints.api_server([ConferenceApi])  # register API

