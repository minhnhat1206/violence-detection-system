import { format, subDays, subHours } from 'date-fns';
import { MOCK_ALERTS, MOCK_CAMERAS } from '../constants';
import { Alert } from '../types';

// Let's make the mock data slightly more dynamic for the "live" effect
const generateRandomAlert = (): Alert => {
    const locations = MOCK_CAMERAS.map(c => c.specificLocation);
    const randomLocation = locations[Math.floor(Math.random() * locations.length)];
    const labels: Array<'Fight' | 'Crowd' | 'Anomaly'> = ['Fight', 'Crowd', 'Anomaly'];
    const randomLabel = labels[Math.floor(Math.random() * labels.length)];
    
    const randomHoursAgo = Math.random() * 24 * 7; // up to 7 days ago
    const timestamp = subHours(new Date(), randomHoursAgo).toISOString();

    return {
        event_id: `EVT-${Math.floor(Math.random() * 10000)}`,
        timestamp,
        location: randomLocation,
        violence_score: 0.5 + Math.random() * 0.5,
        label: randomLabel,
        model_version: 'v2.1.3',
        clip_link: '#',
        status: 'Unreviewed',
    };
};

const dynamicAlerts: Alert[] = [...MOCK_ALERTS];
for (let i = 0; i < 50; i++) {
    dynamicAlerts.push(generateRandomAlert());
}


export const getAnalyticsData = () => {
    // Simulate some new alerts coming in by replacing the oldest with a new one
    if (Math.random() > 0.3) { // 70% chance to update
        dynamicAlerts.shift();
        dynamicAlerts.push(generateRandomAlert());
    }

    // 1. Alerts per Hour (Last 24h)
    const alertsPerHour = Array.from({ length: 24 }, (_, i) => {
        const hourEnd = subHours(new Date(), i);
        const hourStart = subHours(new Date(), i + 1);
        const hourKey = format(hourEnd, 'HH:00');

        const alertsInHour = dynamicAlerts.filter(a => {
            const alertDate = new Date(a.timestamp);
            return alertDate >= hourStart && alertDate < hourEnd;
        });

        return {
            name: hourKey,
            alerts: alertsInHour.length,
        };
    }).reverse();

    // 2. Top 5 Locations by Alert Count
    const topLocationsCount = dynamicAlerts.reduce((acc, alert) => {
        acc[alert.location] = (acc[alert.location] || 0) + 1;
        return acc;
    }, {} as Record<string, number>);

    const topLocations = Object.entries(topLocationsCount)
        .sort(([, a], [, b]) => b - a)
        .slice(0, 5)
        .map(([name, alerts]) => ({ name, alerts }));

    // 3. Distribution by Alert Type
    const alertTypesCount = dynamicAlerts.reduce((acc, alert) => {
        acc[alert.label] = (acc[alert.label] || 0) + 1;
        return acc;
    }, {} as Record<string, number>);

    const alertTypes = Object.entries(alertTypesCount).map(([name, value]) => ({ name, value }));
    
    // 4. Average Violence Score (Last 7 Days)
    const avgScore = Array.from({ length: 7 }, (_, i) => {
        const day = subDays(new Date(), i);
        const dayKey = format(day, 'MMM dd');
        const alertsOnDay = dynamicAlerts.filter(a => {
            const alertDate = new Date(a.timestamp);
            return format(alertDate, 'MMM dd') === dayKey && alertDate <= day;
        });
        
        return {
            name: dayKey,
            score: alertsOnDay.length > 0 ? alertsOnDay.reduce((sum, a) => sum + a.violence_score, 0) / alertsOnDay.length : 0,
        };
    }).reverse();


    return {
        alertsPerHour,
        topLocations,
        alertTypes,
        avgScore,
    };
};
