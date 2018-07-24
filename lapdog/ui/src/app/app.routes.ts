
import { Routes } from '@angular/router';
// import { Component } from "path";
import { HomeComponent } from './pages/home/home';

export const RouterConfig: Routes = [
  { path: '', redirectTo: 'home', pathMatch: 'full'},
  { path: 'home', component: HomeComponent },

];
