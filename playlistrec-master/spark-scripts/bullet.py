from pushbullet import Pushbullet
apiKey = 'o.jsMGG789aqsV47OChBCGlPIJjQGli3Qz'


pb = Pushbullet(apiKey)

xperia = pb.get_device('Xperia M2')

def sendNotificationToMattia(title,body):
    push = xperia.push_note(title, body)

sendNotificationToMattia("Ciao Mattia","Sono pronto!")




