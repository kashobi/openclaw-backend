// Apex Q service worker. Enables install to home screen and push notifications.
// Network first by design so live market data is never stale.

self.addEventListener("install", function(e){
  self.skipWaiting();
});

self.addEventListener("activate", function(e){
  e.waitUntil(self.clients.claim());
});

self.addEventListener("fetch", function(e){
  e.respondWith(
    fetch(e.request).catch(function(){
      return caches.match(e.request);
    })
  );
});

// Push notifications. Used later once login and the alert engine are live.
self.addEventListener("push", function(e){
  var data = {};
  try { data = e.data ? e.data.json() : {}; } catch(err) { data = { title: "Apex Q", body: e.data ? e.data.text() : "" }; }
  var title = data.title || "Apex Q";
  var options = {
    body: data.body || "",
    icon: "/icon-192.png",
    badge: "/icon-192.png",
    data: data.url || "/"
  };
  e.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener("notificationclick", function(e){
  e.notification.close();
  e.waitUntil(clients.openWindow(e.notification.data || "/"));
});
