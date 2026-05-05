
require('dotenv').config?.();
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
const { Client, GatewayIntentBits, Partials } = require('discord.js');
const { joinVoiceChannel, getVoiceConnection, createAudioPlayer, createAudioResource, AudioPlayerStatus, NoSubscriberBehavior, VoiceConnectionStatus, entersState, StreamType } = require('@discordjs/voice');
const fs = require('fs');
const path = require('path');
const { spawn } = require('child_process');
const ffmpegPath = require('ffmpeg-static');
const { v4: uuidv4 } = require('uuid');

const PORT = Number(process.env.PORT || 10000);
const SECRET = process.env.BRIDGE_SECRET || 'change_me';
const PREFIX = process.env.COMMAND_PREFIX || '!sky';
const DEFAULT_SESSION_ID = process.env.DEFAULT_SESSION_ID || 'skyecho-default';
const sessions = new Map(); // sessionId -> Set(ws)
const guildState = new Map(); // guildId -> {connection, player, boundUserId, sessionId, armed, pttMode, speakingSince, cooldownUntil}

function checkSecret(req){ return !SECRET || SECRET==='change_me' || req.headers['x-bridge-secret']===SECRET || req.query.secret===SECRET; }
function sendToSession(sessionId, payload){ const set=sessions.get(sessionId||DEFAULT_SESSION_ID); if(!set) return 0; const msg=JSON.stringify(payload); let n=0; for(const ws of set){ if(ws.readyState===1){ ws.send(msg); n++; }} return n; }
function getState(guildId){ if(!guildState.has(guildId)) guildState.set(guildId,{sessionId:DEFAULT_SESSION_ID,armed:false,pttMode:process.env.DISCORD_PTT_MODE||'hybrid',player:null,connection:null,boundUserId:null,speakingSince:null,cooldownUntil:0}); return guildState.get(guildId); }

const app=express(); app.use(express.json({limit:'10mb'}));
app.get('/health',(req,res)=>res.json({ok:true,service:'SkyEcho v4 Hybrid Console PTT Bridge'}));
app.post('/bridge/event',async(req,res)=>{ if(!checkSecret(req)) return res.status(401).json({ok:false,error:'bad secret'}); const body=req.body||{}; if(body.type==='play_text'){ await playTextToAll(body.text||'', body.role||'atc'); } if(body.type==='armed'||body.type==='config'){ for(const st of guildState.values()){ if(body.sessionId) st.sessionId=body.sessionId; if(typeof body.armed==='boolean') st.armed=body.armed; if(body.pttMode) st.pttMode=body.pttMode; } } res.json({ok:true}); });

const server=http.createServer(app);
const wss=new WebSocketServer({server,path:'/ws'});
wss.on('connection',(ws,req)=>{ const url=new URL(req.url,'http://localhost'); const secret=url.searchParams.get('secret'); const sessionId=url.searchParams.get('sessionId')||DEFAULT_SESSION_ID; if(SECRET && SECRET!=='change_me' && secret!==SECRET){ ws.close(1008,'bad secret'); return; } if(!sessions.has(sessionId)) sessions.set(sessionId,new Set()); sessions.get(sessionId).add(ws); ws.send(JSON.stringify({type:'bridge_connected',sessionId})); ws.on('close',()=>sessions.get(sessionId)?.delete(ws)); });

const client=new Client({intents:[GatewayIntentBits.Guilds,GatewayIntentBits.GuildMessages,GatewayIntentBits.MessageContent,GatewayIntentBits.GuildVoiceStates],partials:[Partials.Channel]});
client.once('ready',()=>console.log(`SkyEcho v4 bridge online as ${client.user.tag}. Prefix ${PREFIX}`));
client.on('messageCreate',async(msg)=>{ if(msg.author.bot||!msg.content.startsWith(PREFIX))return; const args=msg.content.slice(PREFIX.length).trim().split(/\s+/); const cmd=(args.shift()||'').toLowerCase(); const st=getState(msg.guild.id);
  try{
    if(cmd==='join'){ const vc=msg.member?.voice?.channel; if(!vc) return msg.reply('Join a voice channel first.'); const conn=joinVoiceChannel({channelId:vc.id,guildId:vc.guild.id,adapterCreator:vc.guild.voiceAdapterCreator,selfDeaf:false,selfMute:false}); st.connection=conn; st.boundUserId=msg.author.id; st.sessionId=st.sessionId||DEFAULT_SESSION_ID; await waitReady(conn,30000); subscribePlayer(st); setupReceiver(vc.guild.id, conn); msg.reply(`SkyEcho bridge joined ${vc.name}. Bound pilot: ${msg.author.username}.`); sendToSession(st.sessionId,{type:'armed_status',status:'joined',user:msg.author.username}); }
    else if(cmd==='leave'){ const c=getVoiceConnection(msg.guild.id); if(c)c.destroy(); st.connection=null; msg.reply('SkyEcho bridge left voice.'); }
    else if(cmd==='bind'){ const user=msg.mentions.users.first()||msg.author; st.boundUserId=user.id; msg.reply(`Bound Discord PTT pilot to ${user.username}.`); }
    else if(cmd==='bridge'){ const val=(args[0]||'on').toLowerCase(); st.armed=val!=='off'; msg.reply(`Discord armed bridge ${st.armed?'ON':'OFF'}.`); sendToSession(st.sessionId,{type:'armed_status',status:st.armed?'armed':'disarmed'}); }
    else if(cmd==='pttmode'){ st.pttMode=(args[0]||'hybrid').toLowerCase(); msg.reply(`PTT mode set to ${st.pttMode}.`); }
    else if(cmd==='status'){ msg.reply(`Bridge status: armed=${st.armed}, mode=${st.pttMode}, bound=${st.boundUserId||'none'}, session=${st.sessionId}`); }
    else if(cmd==='beep'){ await playBeep(st); msg.reply('Beep sent.'); }
    else if(cmd==='say'){ await playTextToGuild(msg.guild.id,args.join(' ')||'SkyEcho bridge radio check.'); msg.reply('Playback sent.'); }
    else msg.reply('Commands: join, leave, bind, bridge on/off, pttmode hybrid|armed_voice|mute_toggle, status, beep, say <text>');
  }catch(e){ console.error(e); msg.reply('Bridge error: '+e.message); }
});

client.on('voiceStateUpdate',(oldS,newS)=>{ const guildId=(newS.guild||oldS.guild).id; const st=getState(guildId); const uid=newS.id||oldS.id; if(st.boundUserId && uid!==st.boundUserId) return; if(!st.armed && st.pttMode!=='mute_toggle') return; const oldMute=oldS.selfMute, newMute=newS.selfMute; if(oldMute===true && newMute===false){ sendToSession(st.sessionId,{type:'ptt_start',source:'discord_mute_toggle',mode:'mute_toggle',userId:uid}); } if(oldMute===false && newMute===true){ sendToSession(st.sessionId,{type:'ptt_end',source:'discord_mute_toggle',mode:'mute_toggle',userId:uid}); } });

async function waitReady(conn,ms){ console.log('[VOICE] waiting for ready'); conn.on('stateChange',(o,n)=>console.log('[VOICE] state='+n.status)); await entersState(conn,VoiceConnectionStatus.Ready,ms); console.log('[VOICE] connection ready'); }
function subscribePlayer(st){ if(!st.player){ st.player=createAudioPlayer({behaviors:{noSubscriber:NoSubscriberBehavior.Play}}); st.player.on(AudioPlayerStatus.Playing,()=>console.log('[AUDIO] player state: playing')); st.player.on(AudioPlayerStatus.Idle,()=>console.log('[AUDIO] player state: idle')); st.player.on('error',e=>console.error('[AUDIO ERROR]',e)); } if(st.connection){ st.connection.subscribe(st.player); console.log('[AUDIO] connection subscribed to player'); } }
function setupReceiver(guildId,conn){ const st=getState(guildId); if(st.receiverSetup)return; st.receiverSetup=true; conn.receiver.speaking.on('start',(userId)=>{ if(st.boundUserId && userId!==st.boundUserId)return; if(!st.armed && !['hybrid','armed_voice'].includes(st.pttMode))return; if(Date.now()<st.cooldownUntil)return; st.speakingSince=Date.now(); sendToSession(st.sessionId,{type:'ptt_start',source:'discord_voice_activity',mode:st.pttMode,userId}); console.log('[PTT] voice start',userId); }); conn.receiver.speaking.on('end',(userId)=>{ if(st.boundUserId && userId!==st.boundUserId)return; const dur=Date.now()-(st.speakingSince||Date.now()); const min=Number(process.env.MIN_SPEECH_MS||900); if(dur>=min){ sendToSession(st.sessionId,{type:'ptt_end',source:'discord_voice_activity',mode:st.pttMode,userId,durationMs:dur}); console.log('[PTT] voice end',userId,dur); } else { sendToSession(st.sessionId,{type:'ptt_end',source:'discord_voice_activity_ignored_short',mode:st.pttMode,userId,durationMs:dur}); } st.speakingSince=null; }); }
async function playTextToAll(text,role){ for(const [guildId,st] of guildState.entries()) await playTextToGuild(guildId,text,role); }
async function playTextToGuild(guildId,text,role='atc'){ const st=getState(guildId); if(!st.connection) return console.log('[AUDIO] no connection for guild',guildId); subscribePlayer(st); // simple generated test tone pattern, not natural TTS; web app should normally synthesize final audio or use external TTS in later backend.
  await playBeep(st); console.log('[AUDIO] play_text received:',text); st.cooldownUntil=Date.now()+Number(process.env.COOLDOWN_MS||2000); }
async function playBeep(st){ if(!st.connection) throw new Error('not connected to voice'); await waitReady(st.connection,30000).catch(e=>console.error('[AUDIO ERROR] ready before beep',e.message)); subscribePlayer(st); const file=path.join('/tmp',`skyecho-beep-${uuidv4()}.wav`); await makeBeep(file); const resource=createAudioResource(file,{inputType:StreamType.Arbitrary,inlineVolume:true}); if(resource.volume) resource.volume.setVolume(0.75); st.player.play(resource); console.log('[AUDIO] beep play command sent',file); }
function makeBeep(out){ return new Promise((resolve,reject)=>{ const ff=spawn(ffmpegPath,['-y','-f','lavfi','-i','sine=frequency=880:duration=0.35','-ar','48000','-ac','2',out]); ff.on('exit',c=>c===0?resolve():reject(new Error('ffmpeg beep failed '+c))); }); }

server.listen(PORT,'0.0.0.0',()=>console.log('SkyEcho v4 bridge server listening on '+PORT));
if(!process.env.DISCORD_TOKEN){ console.error('DISCORD_TOKEN missing'); process.exit(1); }
client.login(process.env.DISCORD_TOKEN);
