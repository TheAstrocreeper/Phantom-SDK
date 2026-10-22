import test from 'node:test';
import assert from 'node:assert/strict';
import {encodeLanPairingCode,parseLanPairingCode,newLanPairingPassword} from '@phantom-local/doorways/local';
const alphabet='0123456789ABCDEFGHJKMNPQRSTVWXYZ';
const sample={host:'192.168.0.214',port:46139,password:'2W9X7'};
test('LAN code carries its private endpoint and PAKE password without lookup',()=>{
  const code=encodeLanPairingCode(sample);
  assert.match(code,/^[0-9A-Z]{4}(?:-[0-9A-Z]{4}){3}$/);
  assert.deepEqual(parseLanPairingCode(code),sample);
  assert.deepEqual(parseLanPairingCode(code.replaceAll('-',' ').toLowerCase()),sample);
});
test('all RFC1918 ranges and service port boundaries round-trip',()=>{
  for(const host of ['10.0.0.0','10.255.255.255','172.16.0.0','172.31.255.255','192.168.0.0','192.168.255.255'])
    for(const port of [1024,46139,65535])for(const password of ['00000','ZZZZZ']){
      const value={host,port,password};assert.deepEqual(parseLanPairingCode(encodeLanPairingCode(value)),value);
    }
});
test('public, loopback, link-local, hostnames and noncanonical IPs are rejected',()=>{
  for(const host of ['8.8.8.8','127.0.0.1','169.254.1.1','172.15.255.255','172.32.0.0','192.169.0.1','100.64.0.1','localhost','[::1]','192.168.00.214','192.168.1.256','0xc0a800d6','192.168.0.214/path'])
    assert.throws(()=>encodeLanPairingCode({...sample,host}));
  for(const port of [0,1023,65536,-1,3.5,'46139',NaN])assert.throws(()=>encodeLanPairingCode({...sample,port}));
});
test('every single-symbol transcription error is detected',()=>{
  const code=encodeLanPairingCode(sample).replaceAll('-','');
  for(let index=0;index<code.length;index++)for(const symbol of alphabet){
    if(symbol===code[index])continue;
    assert.throws(()=>parseLanPairingCode(code.slice(0,index)+symbol+code.slice(index+1)));
  }
});
test('ambiguous remote-control characters normalize; malformed and excessive input fail',()=>{
  const code=encodeLanPairingCode({...sample,password:'00110'});
  assert.deepEqual(parseLanPairingCode(code.replaceAll('0','O').replaceAll('1','l')),{...sample,password:'00110'});
  for(const input of ['',null,42,'x'.repeat(81),'U'.repeat(16),'０'.repeat(16),code+'A'])assert.throws(()=>parseLanPairingCode(input));
});
test('randomly generated passwords remain 25-bit PAKE inputs and round-trip',()=>{
  for(let i=0;i<100;i++){
    const password=newLanPairingPassword();assert.match(password,/^[0-9A-HJKMNP-TV-Z]{5}$/);
    assert.deepEqual(parseLanPairingCode(encodeLanPairingCode({...sample,password})),{...sample,password});
  }
});
