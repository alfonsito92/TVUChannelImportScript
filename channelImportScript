import xml.etree.ElementTree as ET
import pandas as pd # type: ignore
import random
import string
import hashlib
import time
import json
import http.client
import os

#Functions definition

def generate_request_id(length):
    characters = string.ascii_letters + string.digits
    return ''.join(random.choice(characters) for _ in range(length))

def smpte_to_ms(smpte_time_code):
    try:
        hours, minutes, seconds_frames = smpte_time_code.split(':')
        seconds, frames = seconds_frames.split(frameSplit)
        
        # Convertir el tiempo a segundos
        total_seconds = int(hours) * 3600 + int(minutes) * 60 + int(seconds)
        return total_seconds*1000
    except ValueError as e:
        print(f"Time Format error: {e}")
        return smpte_time_code  # Puedes retornar otro valor que indique error

    except Exception as e:
        print(f"Unexpected error: {e}")
        return None

def bxfToJson(bxf_file):
    tree = ET.parse(bxf_file)
    root = tree.getroot()
    
    # Definir el espacio de nombres para las búsquedas de elementos
    namespaces = {'bxf': 'http://smpte-ra.org/schemas/2021/2015/BXF'}

    # Extraer el valor de 'ScheduleName'
    schedule_name_element = root.find('.//bxf:ScheduleName', namespaces)
    template_name = schedule_name_element.text if schedule_name_element is not None else None

    # Crear una lista para almacenar los detalles de cada evento
    events_details = []

    #Calculo de reference start time
    events = root.findall('.//bxf:ScheduleElements', namespaces)
    referenceStartTime = smpte_to_ms(events[0].find('bxf:EventData', namespaces).find('.//bxf:SmpteTimeCode', namespaces).text)

    # Iterar a través de cada 'ScheduleElements'
    for event in events:
        try:

            event_data = event.find('bxf:EventData', namespaces)
            
            # Extraer los datos del evento
            event_type = event_data.get('eventType')
            event_title = event_data.find('.//bxf:EventTitle', namespaces).text
            start_mode = event_data.find('.//bxf:StartMode', namespaces).text
            end_mode = event_data.find('.//bxf:EndMode', namespaces).text
            
            
            smpte_date_time = event_data.find('.//bxf:SmpteDateTime', namespaces).get('broadcastDate')
            smpte_time_code = event_data.find('.//bxf:SmpteTimeCode', namespaces).text
            
            duration = event_data.find('.//bxf:Duration/bxf:SmpteDuration/bxf:SmpteTimeCode', namespaces).text
            house_number = event.find('.//bxf:Content/bxf:ContentId/bxf:HouseNumber', namespaces).text
            content_id = event.find('.//bxf:ContentId/bxf:AlternateId', namespaces).text
            name = event.find('.//bxf:Name', namespaces).text
            som_time_code = event.find('.//bxf:SOM/bxf:SmpteTimeCode', namespaces).text
            smpte_duration_time_code = event.find('.//bxf:MediaLocation/bxf:Duration/bxf:SmpteDuration/bxf:SmpteTimeCode', namespaces).text

            # Convertir ambos tiempos a segundos
            smpte_mseconds = smpte_to_ms(smpte_time_code)
            duration_mseconds = smpte_to_ms(duration)
            
            # Sumar los segundos
            total_mseconds = smpte_mseconds + duration_mseconds - referenceStartTime

            if start_mode == "Fixed":
                fixedEndMode = True

            # Crear un diccionario con los datos del evento
            event_info = {
                'EventType': event_type,
                'EventTitle': event_title,
                'StartTime': smpte_mseconds-referenceStartTime,
                'EndTime': total_mseconds,
                'ContentId': content_id,
                'HouseNumber': house_number,
                'Duration': duration_mseconds,
                'StartMode': start_mode,
                'EndMode': end_mode,
                'Title': name,
            }

            # Agregar el diccionario a la lista
            events_details.append(event_info)
        except AttributeError as e:
            print()
            #print(f"Error: {e}. It seems some elements are missing or have None values.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    return events_details

def xlsxToJson(file_path):
    df = pd.read_excel(file_path)

    # Filtrar filas desde la fila 5 (omitimos filas anteriores al contenido)
    df = df.iloc[3:]

    # Mapeo de datos usando índices de columna
    json_result = []
    for _, row in df.iterrows():
        if pd.notna(row.iloc[1]) and pd.notna(row.iloc[5]):  # Verificar que EventNumber (columna A, índice 0) no sea NaN
            event = {
                "EventNumber": row.iloc[1],        
                "StartTime": row.iloc[2],           
                "EventName": row.iloc[3],           
                "SourceType": row.iloc[4],         
                "Duration": row.iloc[5],           
                "SourceName": row.iloc[8],         
                "Description": row.iloc[9]         
            }
            json_result.append(event)

    # Convertir el resultado a JSON (array de objetos JSON)
    json_output = json.dumps(json_result, ensure_ascii=False, indent=2)

    # Imprimir o guardar el JSON
    return json_output

def csvToJson(file_path):
    #ReadCSV
    df = pd.read_csv(file_path)
    # ConvertToJson
    json_array = df.to_dict(orient='records')
    return json_array

def processFile (file):
    # Obtener la extensión del fichero
    extension = file.split('.')[-1].lower()
    
    # Seleccionar la función según la extensión
    if extension == 'xlsx':
        return xlsxToJson(file)
    elif extension == 'bxf':
        return bxfToJson(file)
    elif extension == 'csv':
        return csvToJson(file)
    else:
        print(f"extension {extension} not supported.")
        return None

def buildEventsJsonDefault(eventsJson):
    events = []
    for event in eventsJson:
        
        startTime = smpte_to_ms(event[startTimeKey])
        duration = smpte_to_ms(event[durationKey])
        if endTimeKey != "":

            endTime = smpte_to_ms(event[endTimeKey])
        else:
            endTime = startTime+duration

        title = event[eventNameKey]
        type = 0 #0 for Media, 1 for live. Assuming all media for now
        liveSource = False # Assuming all clip sources
        clipSource = True # Assuming all clip sources
        mediaFileName = event[sourceNameKey]

        if event[startModeKey] == "Follow":
            fixedStartMode = False
            followOnStartMode = True
        
        else:
            fixedStartMode = True
            followOnStartMode = False            

        if event[endModeKey] == "Duration":
            fixedEndMode = True
            holdEndMode = False

        else:
            fixedEndMode = False
            holdEndMode = True

        json = {
            "startTime": startTime,
            "endTime": endTime,
            "bitrate": 0,
            "delay": 0,
            "title": title,
            "sourceType": type,
            "duration": duration,
            "fixedStartMode": fixedStartMode,
            "followOnStartMode": followOnStartMode,
            "fixedEndMode": fixedEndMode,
            "holdEndMode": holdEndMode,
            "liveSource": liveSource,
            "clipSource": clipSource,
            "mediaFileName": mediaFileName,
            "scheduleFileEventSegmentList": [
                {
                    "segmentId": 1, #1 For media
                    "title": title,
                    "timingIn": 0,
                    "timingOut": duration,
                    "sourceType": type,
                    "fileName": mediaFileName,
                }
                ]
        }
        
        events.append(json)


    #Force first event to be fixed start
    events[0]["fixedStartMode"] = True
    events[0]["followOnStartMode"] = False

    return events

#Here is all the logic that needs to be updated when customers use custom templates     
def buildEventsJsonCustom(eventsJson):
    #For jsp, they are sending in the template only one event with several subevents.


    subEvents = []
    segmentOrder=0
    #Build main event
    initialStartTime = smpte_to_ms(eventsJson[0][startTimeKey])

    for event in eventsJson:

        absoluteStartTime = smpte_to_ms(event[startTimeKey])
        absoluteEndTime = smpte_to_ms (event[durationKey]) + absoluteStartTime

        startTime =  absoluteStartTime - initialStartTime
        endTime = smpte_to_ms(event[durationKey])+startTime

        title = event[eventNameKey]
        type = 0 #Interesting when segmentId=13
        mediaFileName = event[sourceNameKey]
        #Avoid nan values
        if str(mediaFileName) == 'nan':
            mediaFileName = ""
        if event[sourceTypeKey] == "Clip":
            segmentId=10
        elif event[sourceTypeKey] == "SCTEsplice":
            segmentId=2

        subEvent= {
            "segmentId": segmentId, #1 For media 2 for SCTESplice
            "title": title,
            "timingIn": startTime,
            "timingOut": endTime,
            "sourceType": type,
            "fileName": mediaFileName,
            "segmentOrder": segmentOrder  
        }

        subEvents.append(subEvent)
        segmentOrder=segmentOrder+1
        
    #Force subEvent0
    subEvents[0]["segmentId"] = 1
    totalDuration = smpte_to_ms(eventsJson[len(eventsJson)-1][startTimeKey]) + smpte_to_ms(eventsJson[len(eventsJson)-1][durationKey]) - initialStartTime
    fixedStartMode = True
    fixedEndMode = True
    followOnStartMode = False
    holdEndMode = False
    type = 0 #0 for Media, 1 for live. Assuming all media for now
    liveSource = False # Assuming all clip sources
    clipSource = True # Assuming all clip sources
    title=eventsJson[0][eventNameKey]
    mediaFileName=eventsJson[0][sourceNameKey]
    theEvent = [
            {
                "startTime": initialStartTime,
                "endTime": initialStartTime + totalDuration,
                "bitrate": 0,
                "delay": 0,
                "title": title,
                "sourceType": type,
                "duration": totalDuration,
                "fixedStartMode": fixedStartMode,
                "followOnStartMode": followOnStartMode,
                "fixedEndMode": fixedEndMode,
                "holdEndMode": holdEndMode,
                "liveSource": liveSource,
                "clipSource": clipSource,
                "mediaFileName": mediaFileName,
                "scheduleFileEventSegmentList": subEvents
            }
        ]
    return theEvent

def callChannelAPI(events, userId, programId, SID, templateName):
    access_key = {
        "requestId": "",
        "appkey": "",
        "timestamp": "",
        "signature": "",
    }

    data = {
        "name": templateName,
        "programId": programId,
        "userId": userId,
        "api": True,
        "scheduleFileEventParamList": events
    }
    #print(data)
    # Acceso a la clave generada previamente
    headers = {
        "Cookie": "playoutd3=po3; SID="+str(SID),
        "AccessKey": json.dumps(access_key),
        "User-Agent": "Apidog/1.0.0 (https://apidog.com)",
        "Content-Type": "application/json"
    }

    conn = http.client.HTTPSConnection("channel.tvunetworks.com")
    conn.request("POST", "/tvu-playout/template/addScheduleTemplate", json.dumps(data), headers)
    res = conn.getresponse()
    data = res.read()
    print(data.decode("utf-8"))


#User data, to discuss with Engineering how to import
#File to process
file_path = 'XXXX.csv'
temp=file_path.split("/")
templateName=temp[len(temp)-1]

#To change per user and Channel
account = "cprieto_user@tvu.com"
userId="XXXXXX"
programId="XXXXXXX"
hashedPassword="XXXXX"

#Variable Mapping. Must be adapted per channel and customer. 
#Example for CSV Template
eventNameKey = "title"
eventTypeKey = "eventType"
startTimeKey = "startTime"
endTimeKey = "endTime"
durationKey = "duration"
startModeKey = "startMode"
endModeKey = "endMode"
sourceTypeKey = "sourceType"
sourceNameKey = "sourceName"

SID="XXXXXX"

#Default seconds and frames separator
frameSplit=";"

#To select custom or default logic
custom = False

eventsJson=processFile(file_path)
if custom==True:
    channelEventsJson=buildEventsJsonCustom(eventsJson)
else:
    channelEventsJson=buildEventsJsonDefault(eventsJson)


callChannelAPI(channelEventsJson, userId, programId, SID, templateName) #Final Call
